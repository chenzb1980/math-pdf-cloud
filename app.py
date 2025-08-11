import os, io, uuid, re, shutil, datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import fitz  # pymupdf
from PIL import Image
import pytesseract
import pandas as pd

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
IMG_DIR = os.path.join(BASE_DIR, "images")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)

app = FastAPI(title="Math PDF Extractor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

tasks = {}
executor = ThreadPoolExecutor(max_workers=2)

def classify_question(text):
    t = (text or "").strip()
    if re.search(r'选择|A\.|B\.|C\.|D\.', t):
        return "选择题"
    if re.search(r'[_＿]{2,}|填空|空格', t):
        return "填空题"
    return "解答题"

def update_progress(task_id, percent=None, message=None):
    if task_id not in tasks:
        return
    if percent is not None:
        tasks[task_id]['percent'] = int(percent)
    if message:
        tasks[task_id]['log'].append(f"{datetime.datetime.utcnow().isoformat()} {message}")

def extract_single_pdf_to_df(pdf_path, task_id=None):
    rows = []
    doc = fitz.open(pdf_path)
    total = len(doc)
    for pno in range(total):
        page = doc[pno]
        text = page.get_text("text") or ""
        pix = page.get_pixmap(dpi=150)
        img = Image.open(io.BytesIO(pix.tobytes()))
        try:
            ocr_text = pytesseract.image_to_string(img, lang='chi_sim+eng')
        except Exception:
            ocr_text = pytesseract.image_to_string(img, lang='eng')
        combined_text = (text + "\n" + ocr_text).strip()
        page_img_path = os.path.join(IMG_DIR, f"{os.path.basename(pdf_path)}_p{pno+1}.png")
        img.save(page_img_path)
        image_list = page.get_images(full=True)
        saved_images = []
        for ii, imginfo in enumerate(image_list):
            xref = imginfo[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            ext = base_image.get("ext", "png")
            img_name = f"{os.path.splitext(os.path.basename(pdf_path))[0]}_p{pno+1}_{ii}.{ext}"
            img_path = os.path.join(IMG_DIR, img_name)
            with open(img_path, "wb") as f:
                f.write(image_bytes)
            saved_images.append(img_path)

        chunks = re.split(r'(?m)^(?:\s*\d+[\.\、]|\s*[一二三四五六七八九十]+[、．])', combined_text)
        for idx, chunk in enumerate(chunks):
            chunk = chunk.strip()
            if not chunk:
                continue
            inlined_eqs = re.findall(r'\$([^$]+)\$|\\\(([^)]+)\\\)', chunk)
            eqs = [a or b for a,b in inlined_eqs if a or b]
            rows.append({
                "source_file": os.path.basename(pdf_path),
                "page": pno+1,
                "raw_text": chunk,
                "question_type": classify_question(chunk),
                "inline_equations": ";".join(eqs),
                "local_images": ";".join(saved_images + [page_img_path])
            })
        if task_id:
            pct = int((pno+1)/total*100)
            update_progress(task_id, percent=pct, message=f"processed page {pno+1}/{total}")
    doc.close()  # Close the document to free resources
    df = pd.DataFrame(rows)
    return df

def process_task(task_id, saved_path):
    try:
        tasks[task_id]['status'] = 'processing'
        update_progress(task_id, percent=1, message="start processing")
        df = extract_single_pdf_to_df(saved_path, task_id=task_id)
        out_path = os.path.join(OUTPUT_DIR, f"result_{task_id}.xlsx")
        df.to_excel(out_path, index=False)
        tasks[task_id]['status'] = 'done'
        tasks[task_id]['file'] = out_path
        update_progress(task_id, percent=100, message="finished processing")
    except Exception as e:
        tasks[task_id]['status'] = 'error'
        tasks[task_id]['log'].append(str(e))

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        return JSONResponse({"error": "Only PDF files are allowed"}, status_code=400)
    
    fname = f"{uuid.uuid4().hex}_{file.filename}"
    save_path = os.path.join(UPLOAD_DIR, fname)
    
    # Read file content properly for async upload
    content = await file.read()
    with open(save_path, "wb") as f:
        f.write(content)
    
    task_id = uuid.uuid4().hex
    tasks[task_id] = {
        "status": "queued",
        "percent": 0,
        "log": [f"uploaded {file.filename}"],
        "file": None
    }
    executor.submit(process_task, task_id, save_path)
    return {"task_id": task_id}

@app.get("/progress/{task_id}")
def get_progress(task_id: str):
    t = tasks.get(task_id)
    if not t:
        return JSONResponse({"error": "task not found"}, status_code=404)
    return t

@app.get("/download/{task_id}")
def download_result(task_id: str):
    t = tasks.get(task_id)
    if not t:
        return JSONResponse({"error": "task not found"}, status_code=404)
    if t.get("status") != "done" or not t.get("file"):
        return JSONResponse({"error": "not ready"}, status_code=400)
    return FileResponse(
        t['file'], 
        filename=os.path.basename(t['file']), 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.get("/")  # Changed from @app.route("/") which is Flask syntax
def home():
    return {"message": "Math PDF Cloud API is running!"}

# Removed the Flask import and app creation at the end - this should be FastAPI only

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    