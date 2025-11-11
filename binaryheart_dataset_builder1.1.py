# save this file as binaryheart_dataset_builder.py
import requests, os, time, random, json
import sys
from trafilatura import fetch_url, extract
from tqdm import tqdm
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import deque
import re
import io
from datetime import datetime
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    try:
        import codecs
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        if hasattr(sys.stderr, 'buffer'):
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        # Fallback: set environment variable for UTF-8
        os.environ['PYTHONIOENCODING'] = 'utf-8'

# Optional imports for PDF and advanced processing
# These are optional - the scraper works without them (PDFs will be skipped)
try:
    import pdfplumber  # type: ignore
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    # Silent fail - PDFs will be skipped if not available

try:
    import pytesseract  # type: ignore
    from PIL import Image  # type: ignore
    OCR_SUPPORT = True
except ImportError:
    OCR_SUPPORT = False
    # Silent fail - OCR will be skipped if not available

# Configuration
MAX_DOCUMENTS = 25000
MIN_TEXT_LENGTH = 100
SAVE_INTERVAL = 100  # Save progress every N documents
DELAY_MIN = 0.3  # Reduced from 1.5 for faster scraping
DELAY_MAX = 0.8  # Reduced from 3.0 for faster scraping

# Allowed domains (only crawl these domains)
ALLOWED_DOMAINS = [
    "dell.com",
    "support.hp.com",
    "h10032.www1.hp.com",
    "h30434.www3.hp.com",
    "support.lenovo.com",
    "learn.microsoft.com",
    "ifixit.com",
    "superuser.com",
    "answers.microsoft.com",
    "support.google.com",
    "forums.macrumors.com",
    "discussions.apple.com"
]

# Domains that are problematic (rate limiting, blocking, etc.) - skip or handle differently
PROBLEMATIC_DOMAINS = {
    "superuser.com": True,  # Currently blocking/rate limiting
}

# URL patterns to prioritize (these are likely to be good content)
PRIORITY_PATTERNS = [
    r"/kbdoc/",
    r"/document/",
    r"/solutions/",
    r"/troubleshoot/",
    r"/Device/",
    r"/questions/",
    r"/threads/",
    r"/thread/",
    r"/answer/"
]

# URL patterns to skip (these are usually not useful)
SKIP_PATTERNS = [
    r"/search",
    r"/login",
    r"/register",
    r"/cart",
    r"/checkout",
    # Note: PDFs are now processed (removed from skip list)
    r"\.jpg$|\.png$|\.gif$|\.svg$",  # Skip images
    r"#",  # Skip anchors
]

# Seed URLs to start crawling from
# Strategy: Use "hub" pages (directories, categories, indexes) that link to many other pages
# You only need 20-50 good seed URLs, not 25,000! The crawler will discover links automatically.
SEED_URLS = [
    # --- Dell Support Hub Pages (these link to many KB articles) ---
    "https://www.dell.com/support/kbdoc/en-us",
    "https://www.dell.com/support/manuals/en-us",
    "https://www.dell.com/support/home/en-us",
    
    # --- HP Support Hub Pages ---
    "https://support.hp.com/us-en/document",
    "https://support.hp.com/us-en/help",
    "https://support.hp.com/us-en/drivers",
    
    # --- Lenovo Support Hub Pages ---
    "https://support.lenovo.com/us/en/solutions",
    "https://support.lenovo.com/us/en/documents",
    "https://support.lenovo.com/us/en/",
    
    # --- Microsoft Learn/Troubleshoot Hub Pages ---
    "https://learn.microsoft.com/en-us/troubleshoot/windows-client",
    "https://learn.microsoft.com/en-us/troubleshoot/windows-server",
    "https://learn.microsoft.com/en-us/troubleshoot/office",
    "https://learn.microsoft.com/en-us/troubleshoot/",
    
    # --- iFixit Device Pages (each links to many guides) ---
    "https://www.ifixit.com/Device/Dell_Laptop",
    "https://www.ifixit.com/Device/HP_Laptop",
    "https://www.ifixit.com/Device/Lenovo_Laptop",
    "https://www.ifixit.com/Device/Chromebook",
    "https://www.ifixit.com/Device/MacBook",
    "https://www.ifixit.com/Device/Desktop",
    
    # --- Forum Hub Pages (these link to many discussion threads) ---
    "https://superuser.com/questions/tagged/windows",
    "https://superuser.com/questions/tagged/boot",
    "https://superuser.com/questions/tagged/repair",
    "https://superuser.com/questions/tagged/laptop",
    "https://answers.microsoft.com/en-us/windows/forum",
    "https://answers.microsoft.com/en-us",
    "https://answers.microsoft.com/en-us/windows/forum/windows_10",
    "https://forums.macrumors.com/forums/macbook.89/",
    "https://forums.macrumors.com/forums/",
    "https://discussions.apple.com/community/mac",
    "https://www.dell.com/community/en/topics",
    "https://www.dell.com/community/en/conversations",
    "https://h30434.www3.hp.com/t5/forums/forumtopicpage/board-id",
    
    # --- Google Support Hub ---
    "https://support.google.com/chromebook",
    
    # --- Reddit Tech Support (if you want to include) ---
    # Uncomment if you want to add Reddit (note: may need to add reddit.com to ALLOWED_DOMAINS)
    # "https://www.reddit.com/r/techsupport/",
    # "https://www.reddit.com/r/Windows10/",
    # "https://www.reddit.com/r/applehelp/",
]

# File paths for saved data
# Get the script's directory to ensure we use the correct paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Check if we're in the datascraper folder, if not, use the datascraper folder
if "datascraper v.01" in SCRIPT_DIR:
    BASE_DIR = SCRIPT_DIR
elif os.path.basename(SCRIPT_DIR) == "Desktop" or "Desktop" in SCRIPT_DIR:
    # Script is in Desktop, but data is in datascraper v.01 folder
    BASE_DIR = os.path.join(SCRIPT_DIR, "datascraper v.01")
else:
    # Fallback: use script directory
    BASE_DIR = SCRIPT_DIR

progress_file = os.path.join(BASE_DIR, "data/exports/crawler_progress.json")  # Crawler state (for resuming)
output_file = os.path.join(BASE_DIR, "data/exports/dataset.jsonl")  # Main dataset (JSONL format: one JSON object per line)

# Create data directories in the correct location
os.makedirs(os.path.join(BASE_DIR, "data/raw"), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data/exports"), exist_ok=True)

# Load existing progress if available
visited_urls = set()
url_queue = deque()
records = []
is_resuming = False

# Create session with connection pooling for faster requests
def create_session():
    """Create a requests session with connection pooling and retry strategy"""
    session = requests.Session()
    
    # Configure retry strategy (reduced for speed)
    try:
        retry_strategy = Retry(
            total=1,  # Only retry once for speed
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        # Use HTTPAdapter with connection pooling (reuse connections)
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,  # More connections for parallel requests
            pool_maxsize=50
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    except:
        # If urllib3 not available, use basic session
        pass
    
    # Set default headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })
    
    return session

# Create global session for connection pooling (faster than creating new sessions)
http_session = create_session()

if os.path.exists(progress_file):
    print("📂 Loading previous progress...")
    with open(progress_file, "r", encoding="utf-8") as f:
        progress = json.load(f)
        visited_urls = set(progress.get("visited_urls", []))
        url_queue = deque(progress.get("url_queue", []))
        print(f"   Resuming: {len(visited_urls)} visited, {len(url_queue)} in queue")
        is_resuming = True
else:
    # Starting fresh - clear output file
    if os.path.exists(output_file):
        os.remove(output_file)

# Count existing documents if resuming
existing_docs = 0
if is_resuming and os.path.exists(output_file):
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            existing_docs = sum(1 for line in f if line.strip())
        print(f"   Found {existing_docs} existing documents in output file")
    except:
        pass

# Initialize queue with seed URLs
if not url_queue:
    for url in SEED_URLS:
        if url not in visited_urls:
            url_queue.append(url)

def is_allowed_domain(url):
    """Check if URL is from an allowed domain"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    for allowed in ALLOWED_DOMAINS:
        if allowed in domain:
            return True
    return False

def should_skip_url(url):
    """Check if URL should be skipped"""
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False

def get_url_priority(url):
    """Get priority score for URL (higher = more important)"""
    for pattern in PRIORITY_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return 1
    return 0

def is_404_page(html):
    """Check if HTML content indicates a 404 error page"""
    if not html:
        return False
    
    html_lower = html.lower()
    
    # Common 404 error indicators
    error_indicators = [
        r"404\s*-\s*page\s*not\s*found",
        r"404\s*error",
        r"we\s*couldn't\s*find\s*this\s*page",
        r"the\s*requested\s*page\s*could\s*not\s*be\s*found",
        r"document\s*not\s*currently\s*available",
        r"this\s*page\s*does\s*not\s*exist",
        r"error\s*404",
    ]
    
    # Check for multiple 404 indicators (more reliable)
    match_count = 0
    for pattern in error_indicators:
        if re.search(pattern, html_lower, re.IGNORECASE):
            match_count += 1
    
    # If we find 2+ indicators, it's likely a 404 page
    if match_count >= 2:
        return True
    
    # Also check for specific 404 page patterns
    if "404 - page not found" in html_lower and "we couldn't find this page" in html_lower:
        return True
    
    return False

def is_404_content(text):
    """Check if text content indicates a 404 error"""
    if not text or len(text) < 100:
        return False
    
    text_lower = text.lower()
    
    # Check for 404 error patterns in content
    error_patterns = [
        r"404\s*-\s*page\s*not\s*found",
        r"we\s*couldn't\s*find\s*this\s*page",
        r"document\s*not\s*currently\s*available",
    ]
    
    # If content is short and contains 404 indicators, it's likely an error
    if len(text) < 500:
        for pattern in error_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return True
    
    # Check for repeated 404 messages (common in error pages)
    if text_lower.count("404") >= 2 or text_lower.count("page not found") >= 2:
        return True
    
    return False

def extract_links(html, base_url):
    """Extract all links from HTML"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Convert relative URLs to absolute
            absolute_url = urljoin(base_url, href)
            # Remove fragment
            absolute_url = absolute_url.split('#')[0]
            if absolute_url.startswith('http') and is_allowed_domain(absolute_url):
                if not should_skip_url(absolute_url):
                    links.append(absolute_url)
        return links
    except Exception as e:
        return []

def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep punctuation
    text = re.sub(r'[^\w\s\.\,\;\:\!\?\-\(\)\[\]\"\'\/]', '', text)
    # Strip leading/trailing whitespace
    text = text.strip()
    return text

def extract_pdf_text(url):
    """Extract text from PDF with OCR fallback"""
    if not PDF_SUPPORT:
        return None
    
    try:
        # Use session for connection pooling
        response = http_session.get(url, timeout=20, stream=False)
        if response.status_code != 200:
            return None
        
        pdf_file = io.BytesIO(response.content)
        text_parts = []
        
        # Try pdfplumber first (better text extraction)
        try:
            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
        except:
            # Fallback to OCR if text extraction fails
            if OCR_SUPPORT:
                try:
                    from pdf2image import convert_from_bytes
                    images = convert_from_bytes(response.content)
                    for image in images:
                        ocr_text = pytesseract.image_to_string(image)
                        text_parts.append(ocr_text)
                except:
                    pass
        
        return '\n\n'.join(text_parts) if text_parts else None
    except Exception as e:
        return None

def extract_structured_sections(text, url):
    """Extract structured sections from text (device type, component, symptom, procedure)
    Enhanced for repair-assistant LLM training with technician-specific fields"""
    sections = {
        "device_type": None,
        "component": None,
        "symptom": None,
        "procedure": None,
        "title": None,
        "tools_required": [],
        "difficulty_level": None,
        "safety_warnings": [],
        "error_codes": [],
        "estimated_time": None,
        "brand": None,
        "model": None
    }
    
    # Extract title (usually first line or from URL)
    lines = text.split('\n')
    if lines:
        sections["title"] = lines[0].strip()[:200]
    
    # Pattern-based extraction
    text_lower = text.lower()
    
    # Brand extraction
    brand_patterns = {
        "dell": r"\bdell\b",
        "hp": r"\b(hp|hewlett.?packard)\b",
        "lenovo": r"\blenovo\b",
        "apple": r"\b(apple|macbook|imac|ipad)\b",
        "microsoft": r"\bmicrosoft\b",
        "asus": r"\basus\b",
        "acer": r"\bacer\b",
        "samsung": r"\bsamsung\b",
        "toshiba": r"\btoshiba\b"
    }
    for brand, pattern in brand_patterns.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            sections["brand"] = brand
            break
    
    # Device type patterns
    device_patterns = {
        "laptop": r"\b(laptop|notebook|ultrabook)\b",
        "desktop": r"\b(desktop|pc|computer)\b",
        "tablet": r"\b(tablet|ipad|surface)\b",
        "server": r"\b(server|workstation)\b"
    }
    for device, pattern in device_patterns.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            sections["device_type"] = device
            break
    
    # Component patterns
    component_patterns = [
        (r"\b(battery|power supply|charger)\b", "battery"),
        (r"\b(screen|display|monitor|lcd)\b", "display"),
        (r"\b(keyboard|keypad)\b", "keyboard"),
        (r"\b(touchpad|trackpad|mouse)\b", "input device"),
        (r"\b(motherboard|mainboard)\b", "motherboard"),
        (r"\b(hard drive|hdd|ssd|storage)\b", "storage"),
        (r"\b(ram|memory)\b", "memory"),
        (r"\b(cpu|processor)\b", "processor"),
        (r"\b(gpu|graphics card|video card)\b", "graphics"),
        (r"\b(wifi|wireless|network card)\b", "network"),
        (r"\b(fan|cooling|heatsink)\b", "cooling"),
        (r"\b(port|usb|hdmi|connector)\b", "ports")
    ]
    for pattern, component_name in component_patterns:
        if re.search(pattern, text_lower, re.IGNORECASE):
            sections["component"] = component_name
            break
    
    # Symptom patterns
    symptom_keywords = [
        "won't boot", "not turning on", "black screen", "blue screen", "crash",
        "freeze", "slow", "overheating", "no power", "battery not charging",
        "keyboard not working", "touchpad not working", "wifi not working",
        "sound not working", "display issues", "error message"
    ]
    for keyword in symptom_keywords:
        if keyword in text_lower:
            sections["symptom"] = keyword
            break
    
    # Tools required extraction
    tool_patterns = [
        r"\b(screwdriver|phillips|flathead|torx|hex)\b",
        r"\b(multimeter|voltmeter|ohmmeter)\b",
        r"\b(thermal paste|thermal compound)\b",
        r"\b(spudger|pry tool|opening tool)\b",
        r"\b(soldering iron|solder)\b",
        r"\b(compressed air|air duster)\b",
        r"\b(antistatic|esd|wrist strap)\b",
        r"\b(tweezers|forceps)\b",
        r"\b(flashlight|torch)\b",
        r"\b(cleaning solution|isopropyl alcohol)\b"
    ]
    tools_found = set()
    for pattern in tool_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        tools_found.update([m.lower() for m in matches if m])
    sections["tools_required"] = list(tools_found) if tools_found else []
    
    # Safety warnings extraction
    safety_keywords = [
        "warning", "caution", "danger", "hazard", "electrical shock",
        "battery explosion", "fire risk", "toxic", "disconnect power",
        "unplug", "discharge", "electrostatic", "esd", "high voltage"
    ]
    safety_found = []
    for keyword in safety_keywords:
        if keyword in text_lower:
            # Extract sentence containing safety warning
            sentences = text.split('.')
            for sentence in sentences:
                if keyword in sentence.lower():
                    safety_found.append(sentence.strip()[:200])
                    break
    sections["safety_warnings"] = safety_found[:3] if safety_found else []
    
    # Error codes extraction
    error_patterns = [
        r"error\s+(code|number)?\s*:?\s*([A-Z0-9\-]+)",
        r"([A-Z]{2,}\d{4,})",  # Like BSOD codes
        r"(0x[0-9A-F]{4,})",  # Hex error codes
        r"(\d{4,})",  # Numeric error codes
    ]
    error_codes_found = set()
    for pattern in error_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                error_codes_found.add(match[-1])
            else:
                error_codes_found.add(match)
    sections["error_codes"] = list(error_codes_found)[:5] if error_codes_found else []
    
    # Difficulty level estimation
    complexity_indicators = {
        "beginner": [r"\bsimple\b", r"\beasy\b", r"\bquick\b", r"\bbasic\b"],
        "intermediate": [r"\bmoderate\b", r"\bstandard\b", r"\bnormal\b"],
        "expert": [r"\badvanced\b", r"\bcomplex\b", r"\bdifficult\b", r"\brequires\s+experience\b", r"\bexpert\b", r"\bsoldering\b", r"\bcircuit\b"]
    }
    difficulty_scores = {"beginner": 0, "intermediate": 0, "expert": 0}
    for level, patterns in complexity_indicators.items():
        for pattern in patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                difficulty_scores[level] += 1
    
    # Estimate based on procedure length and complexity
    procedure_count = len(re.findall(r"step\s+\d+", text_lower, re.IGNORECASE))
    if procedure_count > 10 or difficulty_scores["expert"] > 0:
        sections["difficulty_level"] = "expert"
    elif procedure_count > 5 or difficulty_scores["intermediate"] > 0 or len(sections["tools_required"]) > 3:
        sections["difficulty_level"] = "intermediate"
    elif procedure_count > 0 or difficulty_scores["beginner"] > 0:
        sections["difficulty_level"] = "beginner"
    else:
        sections["difficulty_level"] = "intermediate"  # Default
    
    # Estimated time (rough estimate based on procedure steps)
    if procedure_count > 0:
        # Rough estimate: 5-15 minutes per step
        estimated_minutes = procedure_count * 10
        if estimated_minutes < 30:
            sections["estimated_time"] = f"{estimated_minutes} minutes"
        elif estimated_minutes < 60:
            sections["estimated_time"] = f"{estimated_minutes} minutes"
        else:
            hours = estimated_minutes // 60
            minutes = estimated_minutes % 60
            sections["estimated_time"] = f"{hours}h {minutes}m" if minutes > 0 else f"{hours} hours"
    
    # Procedure extraction (look for numbered steps or instructions)
    procedure_patterns = [
        r"(step\s+\d+[:\-]?\s*[^\n]+)",
        r"(\d+[\.\)]\s*[^\n]+)",
        r"(procedure[:\-]?\s*[^\n]+)",
        r"(instructions?[:\-]?\s*[^\n]+)"
    ]
    procedures = []
    for pattern in procedure_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
        procedures.extend(matches[:5])  # Limit to 5 steps
    if procedures:
        sections["procedure"] = "\n".join(procedures[:10])
    
    return sections

def calculate_quality_score(metadata, text_length):
    """Calculate quality score for the record (0-1.0)"""
    score = 0.0
    max_score = 10.0
    
    # Text length (max 2 points)
    if text_length >= 500:
        score += 2.0
    elif text_length >= 200:
        score += 1.5
    elif text_length >= 100:
        score += 1.0
    
    # Device type (1 point)
    if metadata.get("device_type"):
        score += 1.0
    
    # Component (1 point)
    if metadata.get("component"):
        score += 1.0
    
    # Symptom (1 point)
    if metadata.get("symptom"):
        score += 1.0
    
    # Procedure (1 point)
    if metadata.get("procedure"):
        score += 1.0
    
    # Tools required (1 point)
    if metadata.get("tools_required"):
        score += 1.0
    
    # Safety warnings (1 point)
    if metadata.get("safety_warnings"):
        score += 1.0
    
    # Error codes (1 point)
    if metadata.get("error_codes"):
        score += 1.0
    
    # Difficulty level (1 point)
    if metadata.get("difficulty_level"):
        score += 1.0
    
    return min(score / max_score, 1.0)

def generate_technician_question(metadata, question_type="diagnosis"):
    """Generate technician-focused questions"""
    device = metadata.get("device_type", "device")
    component = metadata.get("component", "")
    symptom = metadata.get("symptom", "")
    brand = metadata.get("brand", "")
    
    if question_type == "diagnosis" and symptom:
        if component:
            return f"How do I diagnose {symptom} on {component} in a {brand} {device}?" if brand else f"How do I diagnose {symptom} on {component} in a {device}?"
        return f"How do I diagnose {symptom} on a {brand} {device}?" if brand else f"How do I diagnose {symptom} on a {device}?"
    elif question_type == "repair" and component:
        return f"How do I repair or replace the {component} on a {brand} {device}?" if brand else f"How do I repair or replace the {component} on a {device}?"
    elif question_type == "tools" and metadata.get("tools_required"):
        return f"What tools do I need to fix this {device} issue?"
    elif question_type == "procedure" and metadata.get("procedure"):
        return f"What is the step-by-step procedure to fix this issue?"
    
    # Fallback
    title = metadata.get("title", "this issue")
    return f"How do I fix {title.lower()}?"

def generate_question_response_pairs(text, metadata, url):
    """Generate question/response pairs from text and metadata
    Enhanced for repair-assistant LLM training"""
    pairs = []
    text_length = len(text)
    
    # Calculate quality score
    quality_score = calculate_quality_score(metadata, text_length)
    
    # Skip low-quality records
    if quality_score < 0.3:
        return pairs
    
    # Build enhanced response with technician context
    response_parts = [clean_text(text)]
    
    # Add tools required section if available
    if metadata.get("tools_required"):
        tools_text = "Tools required: " + ", ".join(metadata["tools_required"])
        response_parts.append(f"\n\n{tools_text}")
    
    # Add safety warnings if available
    if metadata.get("safety_warnings"):
        safety_text = "⚠️ Safety warnings: " + " | ".join(metadata["safety_warnings"][:2])
        response_parts.append(f"\n\n{safety_text}")
    
    # Add error codes if available
    if metadata.get("error_codes"):
        error_text = "Error codes: " + ", ".join(metadata["error_codes"])
        response_parts.append(f"\n\n{error_text}")
    
    enhanced_response = "\n".join(response_parts)
    
    # Generate main Q&A pair with technician-focused question
    if text_length >= MIN_TEXT_LENGTH:
        question = generate_technician_question(metadata, "diagnosis")
        
        pairs.append({
            "question": question,
            "response": enhanced_response,
            "metadata": {
                "source_url": url,
                "device_type": metadata.get("device_type"),
                "component": metadata.get("component"),
                "symptom": metadata.get("symptom"),
                "brand": metadata.get("brand"),
                "model": metadata.get("model"),
                "tools_required": metadata.get("tools_required", []),
                "difficulty_level": metadata.get("difficulty_level"),
                "safety_warnings": metadata.get("safety_warnings", []),
                "error_codes": metadata.get("error_codes", []),
                "estimated_time": metadata.get("estimated_time"),
                "quality_score": round(quality_score, 2),
                "extracted_at": datetime.now().isoformat(),
                "content_type": "full_article"
            }
        })
    
    # Generate symptom-specific Q&A if symptom is detected
    if metadata.get("symptom"):
        symptom = metadata["symptom"]
        symptom_question = generate_technician_question(metadata, "diagnosis")
        symptom_response = extract_symptom_section(text, symptom)
        if symptom_response and len(symptom_response) >= 50:
            pairs.append({
                "question": symptom_question,
                "response": symptom_response,
                "metadata": {
                    "source_url": url,
                    "device_type": metadata.get("device_type"),
                    "component": metadata.get("component"),
                    "symptom": symptom,
                    "brand": metadata.get("brand"),
                    "tools_required": metadata.get("tools_required", []),
                    "difficulty_level": metadata.get("difficulty_level"),
                    "quality_score": round(quality_score, 2),
                    "extracted_at": datetime.now().isoformat(),
                    "content_type": "symptom_specific"
                }
            })
    
    # Generate procedure-specific Q&A if procedure is detected
    if metadata.get("procedure"):
        procedure = metadata["procedure"]
        procedure_question = generate_technician_question(metadata, "procedure")
        procedure_response = clean_text(procedure)
        
        # Add context to procedure
        if metadata.get("tools_required"):
            procedure_response += f"\n\nTools needed: {', '.join(metadata['tools_required'])}"
        if metadata.get("estimated_time"):
            procedure_response += f"\n\nEstimated time: {metadata['estimated_time']}"
        
        pairs.append({
            "question": procedure_question,
            "response": procedure_response,
            "metadata": {
                "source_url": url,
                "device_type": metadata.get("device_type"),
                "component": metadata.get("component"),
                "brand": metadata.get("brand"),
                "tools_required": metadata.get("tools_required", []),
                "difficulty_level": metadata.get("difficulty_level"),
                "estimated_time": metadata.get("estimated_time"),
                "quality_score": round(quality_score, 2),
                "extracted_at": datetime.now().isoformat(),
                "content_type": "procedure"
            }
        })
    
    # Generate tools-specific Q&A if tools are mentioned
    if metadata.get("tools_required") and len(metadata["tools_required"]) > 0:
        tools_question = generate_technician_question(metadata, "tools")
        tools_response = f"To fix this issue, you will need the following tools:\n\n"
        tools_response += "\n".join([f"• {tool}" for tool in metadata["tools_required"]])
        if metadata.get("procedure"):
            tools_response += f"\n\nProcedure:\n{clean_text(metadata['procedure'])}"
        
        pairs.append({
            "question": tools_question,
            "response": tools_response,
            "metadata": {
                "source_url": url,
                "device_type": metadata.get("device_type"),
                "component": metadata.get("component"),
                "tools_required": metadata.get("tools_required", []),
                "difficulty_level": metadata.get("difficulty_level"),
                "quality_score": round(quality_score, 2),
                "extracted_at": datetime.now().isoformat(),
                "content_type": "tools_guide"
            }
        })
    
    return pairs

def extract_symptom_section(text, symptom):
    """Extract section of text relevant to a specific symptom"""
    sentences = text.split('.')
    relevant_sentences = []
    symptom_lower = symptom.lower()
    
    for idx, sentence in enumerate(sentences):
        if symptom_lower in sentence.lower():
            relevant_sentences.append(sentence.strip())
            # Get a few sentences after for context
            for i in range(idx + 1, min(idx + 4, len(sentences))):
                if sentences[i].strip():
                    relevant_sentences.append(sentences[i].strip())
            break
    
    return '. '.join(relevant_sentences[:5]) if relevant_sentences else None

def process_content(url, html=None, text=None):
    """Process content from URL and return standardized format"""
    # Check if it's a PDF
    if url.lower().endswith('.pdf'):
        text = extract_pdf_text(url)
        if not text:
            return None
        html = None
    elif not text:
        # Extract text from HTML
        if html:
            # Check for 404 page before processing
            if is_404_page(html):
                return None
            
            try:
                text = extract(html) or ""
            except Exception as e:
                # Fallback to basic extraction if trafilatura fails
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    text = soup.get_text()
                except:
                    return None
        else:
            return None
    
    if not text or len(text) < MIN_TEXT_LENGTH:
        return None
    
    # Clean text
    text = clean_text(text)
    
    # Extract structured sections
    sections = extract_structured_sections(text, url)
    
    # Generate title if not found
    if not sections["title"]:
        lines = text.split('\n')
        sections["title"] = lines[0].strip()[:200] if lines else "Untitled"
    
    # Generate question/response pairs
    qa_pairs = generate_question_response_pairs(text, sections, url)
    
    return qa_pairs if qa_pairs else None

def save_progress():
    """Save current progress to disk"""
    progress = {
        "visited_urls": list(visited_urls),
        "url_queue": list(url_queue),
        "records_count": len(records)
    }
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)

def save_records():
    """Save records to JSONL file"""
    # Append mode to preserve existing records during a single run
    with open(output_file, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    records.clear()  # Clear after saving

print(f"🚀 Starting crawl (target: {MAX_DOCUMENTS} documents)")
print(f"   Queue: {len(url_queue)} URLs")
print(f"   Already visited: {len(visited_urls)} URLs\n")

pbar = tqdm(total=MAX_DOCUMENTS, initial=existing_docs, desc="Crawling")
documents_collected = existing_docs

try:
    while url_queue and documents_collected < MAX_DOCUMENTS:
        # Get next URL (prioritize URLs matching priority patterns)
        current_url = None
        priority_urls = [u for u in url_queue if get_url_priority(u) > 0]
        if priority_urls:
            current_url = priority_urls[0]
            url_queue.remove(current_url)
        else:
            current_url = url_queue.popleft()
        
        if current_url in visited_urls:
            continue
        
        visited_urls.add(current_url)
        
        try:
            # Check if domain is problematic and skip if so
            parsed_url = urlparse(current_url)
            domain = parsed_url.netloc.lower().replace('www.', '')
            if domain in PROBLEMATIC_DOMAINS:
                # Skip problematic domains to avoid wasting time on connection errors
                continue
            
            # Fetch content
            html = None
            if not current_url.lower().endswith('.pdf'):
                # Optimized: Use session with connection pooling for faster requests
                try:
                    # Use session directly for connection pooling (faster than trafilatura fetch_url)
                    response = http_session.get(current_url, timeout=10, allow_redirects=True, stream=False)
                    
                    # Check status code immediately (before processing)
                    if response.status_code == 404:
                        continue  # Skip silently
                    
                    if response.status_code >= 400:
                        continue  # Skip error status codes silently
                    
                    # Get HTML content
                    html = response.text
                    
                except requests.exceptions.Timeout:
                    # Skip timeouts silently (reduce noise)
                    continue
                except requests.exceptions.ConnectionError as e:
                    # Connection errors (connection pool exhausted, DNS, etc.)
                    # Skip silently to avoid spam - these are usually temporary
                    continue
                except requests.exceptions.RequestException as e:
                    # Other request errors - skip silently to reduce noise
                    continue
                except Exception as e:
                    # Skip on other errors
                    continue
            
            # Check if content indicates 404 error page
            if html and is_404_page(html):
                continue  # Skip silently
            
            # Process content with parsing and standardization
            qa_pairs = process_content(current_url, html=html)
            
            # Additional check: if processed content indicates 404, skip it
            if qa_pairs:
                for qa_pair in qa_pairs:
                    response_text = qa_pair.get("response", "").lower()
                    if is_404_content(response_text):
                        qa_pairs = None
                        break
            
            # Save question/response pairs (standardized format)
            if qa_pairs:
                for qa_pair in qa_pairs:
                    records.append(qa_pair)
                    documents_collected += 1
                    pbar.update(1)
                pbar.set_postfix({"collected": documents_collected, "queue": len(url_queue)})
            
            # Extract links for further crawling (only from HTML, not PDFs)
            # Skip link extraction from problematic domains to avoid adding more problematic URLs
            if documents_collected < MAX_DOCUMENTS and html:
                # Check if domain is problematic - skip link extraction
                parsed_url = urlparse(current_url)
                domain = parsed_url.netloc.lower().replace('www.', '')
                should_extract_links = domain not in PROBLEMATIC_DOMAINS
                
                if should_extract_links:
                    try:
                        links = extract_links(html, current_url)
                        # Limit queue size to prevent memory issues
                        MAX_QUEUE_SIZE = 10000
                        links_added = 0
                        for link in links:
                            # Also skip links to problematic domains
                            link_parsed = urlparse(link)
                            link_domain = link_parsed.netloc.lower().replace('www.', '')
                            if link_domain in PROBLEMATIC_DOMAINS:
                                continue  # Skip links to problematic domains
                            
                            if link not in visited_urls and link not in url_queue:
                                if len(url_queue) < MAX_QUEUE_SIZE:
                                    url_queue.append(link)
                                    links_added += 1
                                    # Limit links added per page for speed
                                    if links_added >= 50:  # Max 50 links per page
                                        break
                                else:
                                    break
                    except Exception as e:
                        # If link extraction fails, continue with next URL
                        pass
            
            # Save progress periodically
            if documents_collected % SAVE_INTERVAL == 0:
                save_records()
                save_progress()
                print(f"\n💾 Progress saved: {documents_collected} documents collected")
            
            # Rate limiting (reduced delay for faster scraping)
            # Only delay if we successfully processed a document
            if qa_pairs:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            else:
                # Shorter delay for skipped/invalid pages
                time.sleep(random.uniform(0.1, 0.3))
            
        except requests.exceptions.Timeout:
            # Skip timeouts silently
            continue
        except requests.exceptions.ConnectionError:
            # Skip connection errors silently (connection pool, DNS, etc.)
            continue
        except requests.exceptions.RequestException:
            # Skip request errors silently
            continue
        except Exception as e:
            # Only log unexpected errors (not network/request errors)
            # Skip silently to reduce noise - most errors are network-related
            continue
        
        # Update progress bar
        pbar.set_postfix({"collected": documents_collected, "queue": len(url_queue)})

except KeyboardInterrupt:
    print("\n\n⚠️  Interrupted by user")
    interrupted = True
else:
    # Loop completed normally (reached MAX_DOCUMENTS or ran out of URLs)
    print(f"\n\n✅ Crawling completed! Collected {documents_collected} documents")
    interrupted = False
finally:
    # Always save final progress (ensures no data loss)
    print("\n💾 Saving final data...")
    save_records()  # Save any remaining records in memory
    save_progress()  # Save crawler state
    pbar.close()
    print("   Final save complete!")

# Final deduplication and summary
print("\n🧹  Running final deduplication...")
if os.path.exists(output_file):
    seen_responses = {}
    unique_records = []
    with open(output_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # Skip empty lines
                try:
                    record = json.loads(line)
                    # Deduplicate based on response content (standardized format)
                    response_text = record.get("response", "")[:5000]
                    if response_text and response_text not in seen_responses:
                        seen_responses[response_text] = True
                        unique_records.append(record)
                except json.JSONDecodeError:
                    continue  # Skip malformed lines
    
    # Write deduplicated records
    with open(output_file, "w", encoding="utf-8") as f:
        for r in unique_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    
    print(f"✅ Final dataset: {len(unique_records)} unique question/response pairs")
    print(f"📁 Saved to: {output_file}")
    print(f"📊 Progress file: {progress_file}")
    print(f"📋 Format: {{question, response, metadata}}")
else:
    print(f"⚠️  No output file found. Collected {documents_collected} documents in memory.")
