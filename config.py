from dataclasses import dataclass, field
from pathlib import Path
import os

@dataclass
class Settings:
    base_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent if '__file__' in globals() else Path.cwd())
    data_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    reports_dir: Path = field(init=False)
    
    headless: bool = True
    implicit_wait: int = 5
    explicit_wait: int = 15
    page_load_timeout: int = 60
    script_timeout: int = 30
    rate_limit_min_s: float = 0.0
    rate_limit_max_s: float = 0.0
    max_retries: int = 3

    base_url: str = "https://www.bestbuy.com/"
    laptops_category_path: str = "site/computers-pcs/laptop-computers/abcat0502000.c?id=abcat0502000"
    price_min: int = 500
    price_max: int = 1500
    min_rating: float = 4.0
    top_brands: list[str] = field(default_factory=lambda: ["Apple", "Dell", "HP"])

    raw_json_path: Path = field(init=False)
    excel_output_path: Path = field(init=False)
    pdf_output_path: Path = field(init=False)
    dashboard_html_path: Path = field(init=False)

    chrome_binary: str | None = field(default_factory=lambda: os.getenv("CHROME_BINARY"))
    chromedriver_path: str | None = field(default_factory=lambda: os.getenv("CHROMEDRIVER_PATH"))

    enable_multithreading: bool = True
    threads: int = 8
    batch_size: int = 6  # Added type annotation
    max_detail_pages: int = 0
    max_parallel_browsers=6
    max_pages=25
    max_products=600

    email_enabled: bool = False
    email_to: str | None = field(default_factory=lambda: os.getenv("NOTIFY_EMAIL_TO"))
    email_from: str | None = field(default_factory=lambda: os.getenv("NOTIFY_EMAIL_FROM"))
    smtp_host: str | None = field(default_factory=lambda: os.getenv("SMTP_HOST"))
    smtp_port: int = field(default_factory=lambda: int(os.getenv("SMTP_PORT", "587")))
    smtp_user: str | None = field(default_factory=lambda: os.getenv("SMTP_USER"))
    smtp_password: str | None = field(default_factory=lambda: os.getenv("SMTP_PASSWORD"))

    def __post_init__(self):
        # Set derived paths
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.reports_dir = self.base_dir / "reports"
        
        self.raw_json_path = self.data_dir / "raw_products.json"
        self.excel_output_path = self.reports_dir / "ecommerce_analysis.xlsx"
        self.pdf_output_path = self.reports_dir / "ecommerce_report.pdf"
        self.dashboard_html_path = self.reports_dir / "dashboard.html"
        
        # Create directories if they don't exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

SETTINGS = Settings()