import json

import httpx
import pytest
import respx

from job_monitor.models import CompanyConfig
from job_monitor.sources import (
    AmazonSource,
    AppleSource,
    AshbySource,
    GoogleSource,
    GreenhouseSource,
    LeverSource,
    MicrosoftSource,
    SmartRecruitersSource,
    WorkdaySource,
)


def company(ats_type, ats_config):
    return CompanyConfig(
        slug="acme",
        name="Acme",
        careers_url="https://example.com/jobs",
        ats_type=ats_type,
        ats_config=ats_config,
        industry="tech",
        profiles=["tech"],
        source_verified=True,
    )


@pytest.mark.asyncio
@respx.mock
async def test_greenhouse_adapter():
    respx.get("https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true").mock(
        return_value=httpx.Response(
            200,
            json={
                "jobs": [
                    {
                        "id": 7,
                        "title": "Senior Data Analyst",
                        "location": {"name": "Remote US"},
                        "content": "<p>SQL</p>",
                        "absolute_url": "https://boards.greenhouse.io/acme/jobs/7",
                        "updated_at": "2026-06-17T12:00:00Z",
                    }
                ]
            },
        )
    )
    async with httpx.AsyncClient() as client:
        rows = await GreenhouseSource(
            company("greenhouse", {"board_token": "acme"}), client
        ).fetch()
    assert rows[0].external_job_id == "7"
    assert rows[0].description_raw == "SQL"


@pytest.mark.asyncio
@respx.mock
async def test_lever_adapter():
    respx.get("https://api.lever.co/v0/postings/acme?mode=json").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "x",
                    "text": "Data Analyst",
                    "categories": {"location": "Phoenix, AZ"},
                    "descriptionPlain": "SQL",
                    "hostedUrl": "https://jobs.lever.co/acme/x",
                    "createdAt": 1700000000000,
                }
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        rows = await LeverSource(company("lever", {"site": "acme"}), client).fetch()
    assert len(rows) == 1


@pytest.mark.asyncio
@respx.mock
async def test_ashby_adapter():
    respx.get("https://api.ashbyhq.com/posting-api/job-board/acme").mock(
        return_value=httpx.Response(
            200,
            json={
                "jobs": [
                    {
                        "id": "a",
                        "title": "Analytics Engineer",
                        "location": "Remote US",
                        "descriptionHtml": "<p>dbt</p>",
                        "jobUrl": "https://jobs.ashbyhq.com/acme/a",
                    }
                ]
            },
        )
    )
    async with httpx.AsyncClient() as client:
        rows = await AshbySource(company("ashby", {"board_name": "acme"}), client).fetch()
    assert rows[0].description_raw == "dbt"


@pytest.mark.asyncio
@respx.mock
async def test_smartrecruiters_pagination():
    route = respx.get("https://api.smartrecruiters.com/v1/companies/acme/postings")
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "totalFound": 1,
                "content": [
                    {
                        "id": "s",
                        "name": "BI Analyst",
                        "location": {"city": "Dallas", "region": "TX"},
                        "ref": "https://jobs.smartrecruiters.com/acme/s",
                    }
                ],
            },
        ),
        httpx.Response(200, json={"totalFound": 1, "content": []}),
    ]
    respx.get("https://api.smartrecruiters.com/v1/companies/acme/postings/s").mock(
        return_value=httpx.Response(
            200, json={"jobAd": {"sections": {"jobDescription": {"text": "<p>SQL</p>"}}}}
        )
    )
    async with httpx.AsyncClient() as client:
        rows = await SmartRecruitersSource(
            company("smartrecruiters", {"company_identifier": "acme"}), client
        ).fetch()
    assert len(rows) == 1
    assert "Dallas" in rows[0].location_raw
    assert rows[0].description_raw == "SQL"


@pytest.mark.asyncio
@respx.mock
async def test_workday_searches_and_deduplicates():
    endpoint = "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/External/jobs"
    route = respx.post(endpoint)
    posting = {
        "title": "Senior Data Analyst",
        "externalPath": "/job/Phoenix/Senior-Data-Analyst_R1",
        "locationsText": "Phoenix, AZ",
        "bulletFields": ["R1"],
        "postedOn": "2026-06-17T00:00:00Z",
    }
    route.side_effect = [
        httpx.Response(200, json={"total": 1, "jobPostings": [posting]}),
        httpx.Response(200, json={"total": 1, "jobPostings": [posting]}),
    ]
    cfg = company(
        "workday",
        {
            "endpoint": endpoint,
            "site": "acme.wd1.myworkdayjobs.com",
            "detail_base_url": "https://acme.wd1.myworkdayjobs.com/en-US/External",
            "search_texts": ["data", "analytics"],
        },
    )
    async with httpx.AsyncClient() as client:
        rows = await WorkdaySource(cfg, client).fetch()
    assert len(rows) == 1
    assert rows[0].external_job_id.endswith("_R1")


@pytest.mark.asyncio
@respx.mock
async def test_amazon_search_api():
    endpoint = "https://www.amazon.jobs/en/search.json"
    respx.get(
        endpoint,
        params={
            "base_query": "product designer",
            "loc_query": "United States",
            "offset": 0,
            "result_limit": 100,
        },
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": 1,
                "jobs": [
                    {
                        "id_icims": "123",
                        "title": "Senior Product Designer",
                        "normalized_location": "Seattle, Washington, USA",
                        "description": "<p>AI product design</p>",
                        "basic_qualifications": "<p>Figma</p>",
                        "preferred_qualifications": "",
                        "job_path": "/en/jobs/123/senior-product-designer",
                    }
                ],
            },
        )
    )
    cfg = company("amazon", {"endpoint": endpoint})
    cfg = cfg.model_copy(update={"careers_url": "https://www.amazon.jobs/"})
    async with httpx.AsyncClient() as client:
        rows = await AmazonSource(cfg, client).fetch()
    assert rows[0].external_job_id == "123"
    assert rows[0].description_raw == "AI product design Figma "


@pytest.mark.asyncio
@respx.mock
async def test_microsoft_search_and_detail_api():
    search_endpoint = "https://apply.careers.microsoft.com/api/pcsx/search"
    detail_endpoint = "https://apply.careers.microsoft.com/api/pcsx/position_details"
    respx.get(
        search_endpoint,
        params={
            "domain": "microsoft.com",
            "query": "product designer",
            "location": "United States",
            "start": 0,
        },
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "count": 1,
                    "positions": [
                        {
                            "id": 456,
                            "name": "Product Designer II",
                            "locations": ["United States, Washington, Redmond"],
                            "positionUrl": "/careers/job/456",
                        }
                    ],
                }
            },
        )
    )
    respx.get(
        detail_endpoint,
        params={"position_id": "456", "domain": "microsoft.com", "hl": "en"},
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "id": 456,
                    "name": "Product Designer II",
                    "standardizedLocations": ["Redmond, WA, US"],
                    "jobDescription": "<p>AI experiences and Figma</p>",
                    "positionUrl": "/careers/job/456",
                    "postedTs": 1787332000,
                }
            },
        )
    )
    cfg = company(
        "microsoft",
        {
            "search_endpoint": search_endpoint,
            "detail_endpoint": detail_endpoint,
            "domain": "microsoft.com",
        },
    )
    cfg = cfg.model_copy(update={"careers_url": "https://apply.careers.microsoft.com/"})
    async with httpx.AsyncClient() as client:
        rows = await MicrosoftSource(cfg, client).fetch()
    assert rows[0].location_raw == "Redmond, WA, US"
    assert rows[0].description_raw == "AI experiences and Figma"


@pytest.mark.asyncio
@respx.mock
async def test_google_search_html():
    endpoint = "https://www.google.com/about/careers/applications/jobs/results/"
    respx.get(
        endpoint,
        params={"q": "product designer", "location": "United States", "page": 1},
    ).mock(
        return_value=httpx.Response(
            200,
            text="""
            <base href="/about/careers/applications/">
            <ul><li class="lLd3Je">
              <h3>Staff AI Product Designer</h3>
              <span class="r0wTof">Mountain View, CA, USA</span>
              <p>Design conversational AI experiences with Figma.</p>
              <a href="jobs/results/789-staff-ai-product-designer">Learn more</a>
            </li></ul>
            """,
            request=httpx.Request("GET", endpoint),
        )
    )
    cfg = company("google", {"max_pages": 1})
    cfg = cfg.model_copy(update={"careers_url": endpoint})
    async with httpx.AsyncClient() as client:
        rows = await GoogleSource(cfg, client).fetch()
    assert rows[0].external_job_id == "789"
    assert rows[0].location_raw == "Mountain View, CA, USA"
    assert str(rows[0].url).startswith(
        "https://www.google.com/about/careers/applications/jobs/results/789"
    )


@pytest.mark.asyncio
@respx.mock
async def test_apple_search_hydration():
    endpoint = (
        "https://jobs.apple.com/en-us/search"
        "?location=united-states-USA&team=human-interface-design-DESGN-HID"
    )
    page_url = endpoint + "&page=1"
    payload = {
        "loaderData": {
            "search": {
                "totalRecords": 1,
                "searchResults": [
                    {
                        "id": "2001-0836",
                        "reqId": "2001-0836",
                        "postingTitle": "Senior Product Designer, AI/ML Tools",
                        "transformedPostingTitle": "senior-product-designer-ai-ml-tools",
                        "jobSummary": "Design AI evaluation workflows with Figma.",
                        "postDateInGMT": "2026-08-01T00:00:00Z",
                        "locations": [
                            {
                                "name": "Cupertino",
                                "stateProvince": "California",
                                "countryName": "United States of America",
                            }
                        ],
                    }
                ],
            }
        }
    }
    encoded = json.dumps(json.dumps(payload))
    respx.get(page_url).mock(
        return_value=httpx.Response(
            200,
            text=f"<script>window.__staticRouterHydrationData = JSON.parse({encoded});</script>",
        )
    )
    cfg = company("apple", {"max_pages": 1})
    cfg = cfg.model_copy(update={"careers_url": endpoint})
    async with httpx.AsyncClient() as client:
        rows = await AppleSource(cfg, client).fetch()
    assert rows[0].external_job_id == "2001-0836"
    assert "Cupertino" in rows[0].location_raw
    assert rows[0].posted_at is not None
