# ... imports remain the same ...

class SeleniumAgent:
    def __init__(self):
        self.filter = LCDSFilter()
        
        # Chrome Options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage") # Critical for Docker/Actions
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.options = options

    def get_driver(self):
        """Robust driver loader: Tries system path first (GitHub Actions), then Manager."""
        try:
            # 1. Try generic Service (works if chromedriver is in PATH)
            return webdriver.Chrome(options=self.options)
        except:
            # 2. Fallback to Manager (Local Machine)
            logging.info("System driver not found, using WebDriverManager...")
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=self.options)

    def scrape(self, target):
        driver = None
        events = []
        name = target['name']
        
        try:
            driver = self.get_driver()  # <--- Use the robust loader
            
            url = target.get('url')
            if not url:
                logging.info(f"🔎 {name}: Scouting...")
                scout = SearchScout()
                url = scout.find_calendar_url(target['domain'], target['search_query'])
            
            if not url:
                return []

            logging.info(f"🌐 {name}: Visiting {url}")
            driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # ... (Rest of your parsing logic remains exactly the same) ...

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        finally:
            if driver:
                driver.quit()
        return events
