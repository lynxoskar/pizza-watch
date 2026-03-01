Based on the current state of the web and the **Pizzint.watch** platform, here is a comprehensive investigation, testing document, and implementation guide designed specifically to be fed to an LLM coding agent (like GitHub Copilot, Cursor, or an autonomous Python script).

### Phase 1: Investigation & Scraping Strategy

Since Pizzint.watch is a modern web application (and its official API is still in development), it almost certainly relies on JavaScript to render its data dynamically (fetching from an internal backend or Firebase/Supabase instance).

**The Best Approach:**
Instead of raw HTML parsing (which will likely fail if the DOM is rendered client-side), the agent should use a **Network Interception strategy** combined with a headless browser.

1. **Primary Strategy (Network Interception):** Use a tool like **Playwright** to listen to the background XHR/Fetch requests the site makes as it loads. Capturing the raw JSON payload is significantly more reliable than scraping HTML tags.
2. **Fallback Strategy (DOM Scraping):** If the data is pre-rendered (SSR), use Playwright to wait for specific CSS selectors (e.g., the "Doughcon" level container or the pizza shop lists) and extract the text content.

---

### Phase 2: Tests Document

*Provide these test cases to your LLM agent so it knows what success looks like.*

**Test Suite: `test_pizzint_scraper.py**`

| Test Case | Description | Expected Outcome |
| --- | --- | --- |
| `test_site_connectivity` | Ping `https://www.pizzint.watch/` to ensure it is not blocking automated requests (e.g., checking for Cloudflare 403 errors). | HTTP 200 OK. If Cloudflare is detected, the agent must implement stealth headers or `playwright-stealth`. |
| `test_network_interception` | Launch Playwright, navigate to the site, and monitor all network traffic for JSON responses containing keywords like `busyness`, `doughcon`, `threat_level`, or `location`. | A JSON object is successfully intercepted and captured in memory. |
| `test_dom_fallback_extraction` | Wait for the DOM state `networkidle`. Query the DOM for the overarching Threat Level element (e.g., "DOUGHCON X"). | Returns an integer between 1 and 5 (or the specific metric used by the site). |
| `test_data_schema_validation` | Validate the scraped output against a Pydantic model to ensure it has: `timestamp`, `overall_threat_level`, and a list of `monitored_locations` (with name and traffic %). | Data passes validation and exports successfully to JSON/CSV. |

---

### Phase 3: Detailed Implementation Description (Prompt for the LLM Agent)

Copy and paste the following prompt directly into your LLM coding assistant to generate the robust scraper:

---

**[START OF PROMPT FOR LLM AGENT]**

**Role & Objective:**
You are an expert Python data engineer. Your task is to write a robust, asynchronous Python web scraper to extract real-time geopolitical OSINT data from `https://www.pizzint.watch/`.

**Tech Stack Required:**

* `asyncio`
* `playwright` (async API) for headless browsing and network interception.
* `pydantic` for data validation.
* `loguru` for structured logging.

**Implementation Steps & Architecture:**

**1. Data Models (Pydantic):**
Create Pydantic models to structure the output:

* `PizzaLocation`: fields for `name` (str), `busyness_percentage` (int/float), and `status` (str).
* `PizzintData`: fields for `timestamp` (datetime), `doughcon_level` (int/str), and `locations` (List[PizzaLocation]).

**2. Playwright Setup & Stealth:**

* Initialize an async Playwright chromium browser.
* Set a standard User-Agent (mimicking a real Windows/Chrome user) to avoid basic bot detection.

**3. Network Interception (The Core Logic):**

* Before calling `page.goto()`, set up an event listener: `page.on("response", handle_response)`.
* The `handle_response` function should check if the response URL contains API-like endpoints (e.g., `/api/`, `firebase`, `supabase`, or `.json`) and if the response headers indicate `application/json`.
* If a relevant JSON response is intercepted, attempt to parse it for busyness metrics or threat levels, and save it to a global/class variable.

**4. DOM Parsing Fallback (If Network Interception yields nothing):**

* Navigate to `https://www.pizzint.watch/` and await `networkidle`.
* Inspect the page content. Write generic XPath or CSS selectors to find:
* The main threat level indicator (look for text containing "DOUGHCON" or "Index").
* The list of pizza locations and their traffic percentages.


* Extract the inner text and clean the strings.

**5. Execution & Output:**

* Combine the data (prioritizing the intercepted JSON, falling back to DOM data).
* Validate the payload using the `PizzintData` Pydantic model.
* Save the final output to a file named `pizzint_export_[timestamp].json`.

**Error Handling Requirements:**

* Implement explicit timeouts (`timeout=30000`).
* Catch Playwright `TimeoutError` and log it via `loguru`.
* If the site utilizes Cloudflare Turnstile or aggressive bot protection, log a specific warning advising the implementation of `playwright-stealth`.

Please write the complete, executable `scraper.py` script based on these exact specifications.

**[END OF PROMPT FOR LLM AGENT]**

---

### How to use this:

1. Ensure your environment has Python installed.
2. Feed the prompt above to your AI coding agent.
3. Once the code is generated, run `pip install playwright pydantic loguru` and `playwright install chromium` to set up your environment.
4. Run the script. If the site's layout changes or they finally release their official API (which they have stated is in their roadmap), you can seamlessly swap out the Playwright logic for a standard `requests` or `httpx` API call.