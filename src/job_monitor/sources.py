from __future__ import annotations

import asyncio
import html
import json
import re
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .models import AtsType, CompanyConfig, RawJob


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, tz=UTC)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _html_text(value: str | None) -> str:
    return BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True)


class SourceError(RuntimeError):
    pass


def _parse_apple_hydration(page: str) -> dict[str, Any]:
    match = re.search(
        r'window\.__staticRouterHydrationData\s*=\s*JSON\.parse\((".*?")\);',
        page,
        flags=re.DOTALL,
    )
    if not match:
        raise SourceError("Apple careers hydration data not found")
    try:
        return json.loads(json.loads(match.group(1)))
    except json.JSONDecodeError as exc:
        raise SourceError("Apple careers hydration data is invalid") from exc


class JobSource(ABC):
    def __init__(self, company: CompanyConfig, client: httpx.AsyncClient):
        self.company = company
        self.client = client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.HTTPError, SourceError)),
        reraise=True,
    )
    async def get_json(self, url: str, **kwargs: Any) -> Any:
        response = await self.client.get(url, **kwargs)
        response.raise_for_status()
        return response.json()

    @abstractmethod
    async def fetch(self) -> list[RawJob]: ...


class GreenhouseSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        token = self.company.ats_config["board_token"]
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        payload = await self.get_json(url)
        return [
            RawJob(
                source_company=self.company.slug,
                external_job_id=str(item["id"]),
                title=item["title"],
                location_raw=(item.get("location") or {}).get("name", ""),
                description_raw=_html_text(item.get("content")),
                posted_at=_parse_datetime(item.get("updated_at")),
                url=item["absolute_url"],
                metadata={"departments": item.get("departments", [])},
            )
            for item in payload.get("jobs", [])
        ]


class LeverSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        site = self.company.ats_config["site"]
        payload = await self.get_json(f"https://api.lever.co/v0/postings/{site}?mode=json")
        return [
            RawJob(
                source_company=self.company.slug,
                external_job_id=str(item["id"]),
                title=item["text"],
                location_raw=(item.get("categories") or {}).get("location", ""),
                description_raw=_html_text(item.get("descriptionPlain") or item.get("description")),
                posted_at=_parse_datetime(item.get("createdAt")),
                url=item.get("hostedUrl") or item["applyUrl"],
                metadata={"categories": item.get("categories", {})},
            )
            for item in payload
        ]


class AshbySource(JobSource):
    async def fetch(self) -> list[RawJob]:
        board = self.company.ats_config["board_name"]
        payload = await self.get_json(f"https://api.ashbyhq.com/posting-api/job-board/{board}")
        return [
            RawJob(
                source_company=self.company.slug,
                external_job_id=str(item.get("id") or item.get("jobUrl", "")),
                title=item["title"],
                location_raw=item.get("location", ""),
                description_raw=_html_text(
                    item.get("descriptionHtml") or item.get("descriptionPlain")
                ),
                posted_at=_parse_datetime(item.get("publishedAt")),
                url=item.get("jobUrl") or item["applyUrl"],
                metadata={"department": item.get("department")},
            )
            for item in payload.get("jobs", [])
        ]


class SmartRecruitersSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        identifier = self.company.ats_config["company_identifier"]
        base = f"https://api.smartrecruiters.com/v1/companies/{identifier}/postings"
        offset = 0
        jobs: list[RawJob] = []
        while True:
            payload = await self.get_json(base, params={"limit": 100, "offset": offset})
            content = payload.get("content", [])
            for item in content:
                detail = await self.get_json(f"{base}/{item['id']}")
                location = item.get("location") or {}
                sections = (detail.get("jobAd") or {}).get("sections") or {}
                description = " ".join(
                    _html_text(section.get("text"))
                    for section in sections.values()
                    if isinstance(section, dict)
                )
                jobs.append(
                    RawJob(
                        source_company=self.company.slug,
                        external_job_id=str(item["id"]),
                        title=item["name"],
                        location_raw=", ".join(
                            str(location.get(key, ""))
                            for key in ("city", "region", "country")
                            if location.get(key)
                        ),
                        description_raw=description,
                        posted_at=_parse_datetime(item.get("releasedDate")),
                        url=item.get("ref")
                        or f"https://jobs.smartrecruiters.com/{identifier}/{item['id']}",
                    )
                )
            offset += len(content)
            if not content or offset >= int(payload.get("totalFound", offset)):
                break
        return jobs


class WorkdaySource(JobSource):
    async def fetch(self) -> list[RawJob]:
        cfg = self.company.ats_config
        endpoint = cfg["endpoint"]
        site = cfg["site"]
        limit = int(cfg.get("limit", 20))
        jobs: list[RawJob] = []
        seen: set[str] = set()
        search_texts = cfg.get("search_texts") or [""]
        for search_text in search_texts:
            offset = 0
            while True:
                response = await self.client.post(
                    endpoint,
                    json={
                        "appliedFacets": {},
                        "limit": limit,
                        "offset": offset,
                        "searchText": search_text,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                postings = payload.get("jobPostings", [])
                for item in postings:
                    external_path = item.get("externalPath", "")
                    if external_path in seen:
                        continue
                    seen.add(external_path)
                    detail_url = cfg.get("detail_base_url", "").rstrip("/") + external_path
                    description = " ".join(item.get("bulletFields", []))
                    if cfg.get("detail_api_base"):
                        detail_response = await self.client.get(
                            cfg["detail_api_base"].rstrip("/") + external_path
                        )
                        detail_response.raise_for_status()
                        detail = detail_response.json().get("jobPostingInfo", {})
                        description = _html_text(detail.get("jobDescription") or description)
                    jobs.append(
                        RawJob(
                            source_company=self.company.slug,
                            external_job_id=external_path,
                            title=item["title"],
                            location_raw=item.get("locationsText", ""),
                            description_raw=description,
                            posted_at=_parse_datetime(item.get("postedOn")),
                            url=detail_url or f"https://{site}{external_path}",
                            metadata={"workday": item},
                        )
                    )
                offset += len(postings)
                if not postings or offset >= int(payload.get("total", offset)):
                    break
        return jobs


class AmazonSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        cfg = self.company.ats_config
        endpoint = cfg["endpoint"]
        search_texts = cfg.get("search_texts") or ["product designer"]
        location = cfg.get("location", "United States")
        limit = int(cfg.get("limit", 100))
        jobs: list[RawJob] = []
        seen: set[str] = set()
        for search_text in search_texts:
            offset = 0
            while True:
                payload = await self.get_json(
                    endpoint,
                    params={
                        "base_query": search_text,
                        "loc_query": location,
                        "offset": offset,
                        "result_limit": limit,
                    },
                )
                postings = payload.get("jobs", [])
                for item in postings:
                    external_id = str(item.get("id_icims") or item.get("id") or "")
                    if not external_id or external_id in seen:
                        continue
                    seen.add(external_id)
                    description = " ".join(
                        _html_text(item.get(field))
                        for field in (
                            "description",
                            "basic_qualifications",
                            "preferred_qualifications",
                        )
                    )
                    jobs.append(
                        RawJob(
                            source_company=self.company.slug,
                            external_job_id=external_id,
                            title=item["title"],
                            location_raw=item.get("normalized_location")
                            or item.get("location", ""),
                            description_raw=description,
                            url=urljoin(str(self.company.careers_url), item["job_path"]),
                            metadata={"amazon": item},
                        )
                    )
                offset += len(postings)
                total = int(payload.get("hits", offset))
                if not postings or offset >= total:
                    break
        return jobs


class MicrosoftSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        cfg = self.company.ats_config
        search_texts = cfg.get("search_texts") or ["product designer"]
        location = cfg.get("location", "United States")
        jobs: list[RawJob] = []
        seen: set[str] = set()
        for search_text in search_texts:
            start = 0
            while True:
                payload = await self.get_json(
                    cfg["search_endpoint"],
                    params={
                        "domain": cfg["domain"],
                        "query": search_text,
                        "location": location,
                        "start": start,
                    },
                )
                data = payload.get("data") or {}
                positions = data.get("positions", [])
                for item in positions:
                    external_id = str(item["id"])
                    if external_id in seen:
                        continue
                    seen.add(external_id)
                    detail_payload = await self.get_json(
                        cfg["detail_endpoint"],
                        params={
                            "position_id": external_id,
                            "domain": cfg["domain"],
                            "hl": "en",
                        },
                    )
                    detail = detail_payload.get("data") or item
                    public_path = detail.get("positionUrl") or item.get("positionUrl", "")
                    jobs.append(
                        RawJob(
                            source_company=self.company.slug,
                            external_job_id=external_id,
                            title=detail.get("name") or item["name"],
                            location_raw=", ".join(
                                detail.get("standardizedLocations")
                                or detail.get("locations")
                                or item.get("standardizedLocations")
                                or item.get("locations")
                                or []
                            ),
                            description_raw=_html_text(detail.get("jobDescription")),
                            posted_at=_parse_datetime(detail.get("postedTs")),
                            url=detail.get("publicUrl")
                            or urljoin(str(self.company.careers_url), public_path),
                            metadata={"microsoft": detail},
                        )
                    )
                start += len(positions)
                total = int(data.get("count", start))
                if not positions or start >= total:
                    break
        return jobs


class GoogleSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        cfg = self.company.ats_config
        search_texts = cfg.get("search_texts") or ["product designer"]
        location = cfg.get("location", "United States")
        max_pages = int(cfg.get("max_pages", 2))
        jobs: list[RawJob] = []
        seen: set[str] = set()
        for search_text in search_texts:
            for page in range(1, max_pages + 1):
                response = await self.client.get(
                    str(self.company.careers_url),
                    params={"q": search_text, "location": location, "page": page},
                )
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                base_node = soup.find("base", href=True)
                base_url = (
                    urljoin(str(response.url), str(base_node["href"]))
                    if base_node
                    else str(response.url)
                )
                page_jobs = 0
                for card in soup.select("li.lLd3Je"):
                    title_node = card.find("h3")
                    link = card.find("a", href=re.compile(r"jobs/results/"))
                    if not title_node or not link:
                        continue
                    id_match = re.search(r"jobs/results/(\d+)", str(link.get("href", "")))
                    if not id_match or id_match.group(1) in seen:
                        continue
                    external_id = id_match.group(1)
                    seen.add(external_id)
                    page_jobs += 1
                    locations = [
                        node.get_text(" ", strip=True).lstrip("; ")
                        for node in card.select("span.r0wTof")
                    ]
                    jobs.append(
                        RawJob(
                            source_company=self.company.slug,
                            external_job_id=external_id,
                            title=title_node.get_text(" ", strip=True),
                            location_raw="; ".join(dict.fromkeys(locations)),
                            description_raw=card.get_text(" ", strip=True),
                            url=urljoin(base_url, str(link["href"])),
                            metadata={"google_search": search_text},
                        )
                    )
                if not page_jobs:
                    break
        return jobs


class AppleSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        max_pages = int(self.company.ats_config.get("max_pages", 3))
        jobs: list[RawJob] = []
        seen: set[str] = set()
        for page in range(1, max_pages + 1):
            url = httpx.URL(str(self.company.careers_url)).copy_set_param("page", page)
            response = await self.client.get(url)
            response.raise_for_status()
            hydration = _parse_apple_hydration(html.unescape(response.text))
            search = (hydration.get("loaderData") or {}).get("search") or {}
            postings = search.get("searchResults", [])
            for item in postings:
                external_id = str(item.get("reqId") or item.get("id") or "")
                if not external_id or external_id in seen:
                    continue
                seen.add(external_id)
                locations = [
                    ", ".join(
                        value
                        for value in (
                            location.get("name", ""),
                            location.get("stateProvince", ""),
                            location.get("countryName", ""),
                        )
                        if value
                    )
                    for location in item.get("locations", [])
                ]
                slug = item.get("transformedPostingTitle", "job")
                jobs.append(
                    RawJob(
                        source_company=self.company.slug,
                        external_job_id=external_id,
                        title=item["postingTitle"],
                        location_raw="; ".join(locations),
                        description_raw=item.get("jobSummary", ""),
                        posted_at=_parse_datetime(item.get("postDateInGMT")),
                        url=urljoin(
                            str(self.company.careers_url),
                            f"/en-us/details/{external_id}/{slug}",
                        ),
                        metadata={"apple": item},
                    )
                )
            total = int(search.get("totalRecords", len(jobs)))
            if not postings or len(jobs) >= total:
                break
        return jobs


class JsonLdSource(JobSource):
    async def fetch(self) -> list[RawJob]:
        response = await self.client.get(str(self.company.careers_url))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        found: list[dict[str, Any]] = []
        for node in soup.select('script[type="application/ld+json"]'):
            try:
                value = json.loads(node.string or "null")
            except json.JSONDecodeError:
                continue
            candidates = value if isinstance(value, list) else [value]
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("@type") == "JobPosting":
                    found.append(candidate)
                elif isinstance(candidate, dict) and isinstance(candidate.get("@graph"), list):
                    found.extend(x for x in candidate["@graph"] if x.get("@type") == "JobPosting")
        jobs = []
        for item in found:
            location = item.get("jobLocation") or item.get("applicantLocationRequirements") or ""
            if isinstance(location, (dict, list)):
                location = json.dumps(location, ensure_ascii=False)
            jobs.append(
                RawJob(
                    source_company=self.company.slug,
                    external_job_id=str(
                        item.get("identifier", {}).get("value") or item.get("url", "")
                    ),
                    title=item["title"],
                    location_raw=str(location),
                    description_raw=_html_text(item.get("description")),
                    posted_at=_parse_datetime(item.get("datePosted")),
                    url=item.get("url") or str(self.company.careers_url),
                    metadata={"jsonld": item},
                )
            )
        return jobs


SOURCE_CLASSES: dict[AtsType, type[JobSource]] = {
    AtsType.AMAZON: AmazonSource,
    AtsType.APPLE: AppleSource,
    AtsType.GREENHOUSE: GreenhouseSource,
    AtsType.GOOGLE: GoogleSource,
    AtsType.LEVER: LeverSource,
    AtsType.MICROSOFT: MicrosoftSource,
    AtsType.ASHBY: AshbySource,
    AtsType.SMARTRECRUITERS: SmartRecruitersSource,
    AtsType.WORKDAY: WorkdaySource,
    AtsType.JSONLD: JsonLdSource,
}


class SourceRunner:
    def __init__(self, client: httpx.AsyncClient, max_concurrency: int = 5):
        self.client = client
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.domain_locks: dict[str, asyncio.Lock] = {}

    async def fetch(self, company: CompanyConfig) -> list[RawJob]:
        domain = httpx.URL(str(company.careers_url)).host or company.slug
        lock = self.domain_locks.setdefault(domain, asyncio.Lock())
        async with self.semaphore, lock:
            source = SOURCE_CLASSES[company.ats_type](company, self.client)
            return await source.fetch()
