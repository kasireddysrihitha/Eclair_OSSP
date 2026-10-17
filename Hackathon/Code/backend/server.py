#!/usr/bin/env python3
"""Local FileDedup dashboard API with review-first, reversible quarantine."""
import hashlib, json, os, re, shutil, subprocess, time, uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse
ROOT=Path(__file__).resolve().parent.parent; FRONTEND=ROOT/"frontend"; SCANNER=ROOT/"backend"/"filededup"
HOST="127.0.0.1"; PORT=8088; MAX_BODY=65536; MAX_OUTPUT=5_000_000
DATA=Path.home()/".local"/"share"/"filededup"; QUARANTINE=DATA/"quarantine"; MANIFEST=DATA/"manifest.json"
LAST_REPORT=None

def parse_size(text):
 m=re.fullmatch(r"\s*([\d.]+)\s*(B|KiB|MiB|GiB)\s*",text,re.I)
 if not m:return 0
 return int(float(m.group(1))*{"b":1,"kib":1024,"mib":1024**2,"gib":1024**3}[m.group(2).lower()])
def label_size(n):return f"{n/1048576:.2f} MiB" if n>=1048576 else f"{n/1024:.2f} KiB" if n>=1024 else f"{n} B"
def parse_report(output):
 def number(p,d=0):
  m=re.search(p,output);return int(m.group(1)) if m else d
 total=re.search(r"Total readable file size\s*:\s*([^\n]+)",output); saved=re.search(r"Potential storage savings\s*:\s*([^\n]+)",output)
 groups=[];current=None;role=None
 for line in output.splitlines():
  h=re.search(r"GROUP\s+(\d+)\s*\|\s*(\d+)\s+identical-content files",line)
  if h:current={"id":int(h.group(1)),"count":int(h.group(2)),"sizeBytes":0,"files":[]};groups.append(current);role=None;continue
  sm=re.search(r"Size per file:\s*(.+)",line)
  if sm and current is not None:current["sizeBytes"]=parse_size(sm.group(1));continue
  if "[REFERENCE COPY]" in line:role="reference";continue
  if "[REVIEW COPY]" in line:role="review";continue
  text=line.strip()
  if role and current is not None and text and not text.startswith(("+","-")):
   current["files"].append({"role":role,"path":text,"name":Path(text).name});role=None
 for g in groups:g["sizeLabel"]=label_size(g["sizeBytes"])
 return {"filesScanned":number(r"Files successfully hashed\s*:\s*(\d+)"),"totalBytes":parse_size(total.group(1)) if total else 0,"duplicateGroups":number(r"Duplicate groups\s*:\s*(\d+)"),"extraCopies":number(r"Extra copies\s*:\s*(\d+)"),"potentialSavingsBytes":parse_size(saved.group(1)) if saved else 0,"filesSkipped":number(r"Files skipped\s*:\s*(\d+)"),"directoryErrors":number(r"Directory errors\s*:\s*(\d+)"),"scanStatus":"Incomplete" if "INCOMPLETE - FILE LIMIT" in output or "LIMIT REACHED" in output else "Completed","groups":groups}
def sha256(path):
 h=hashlib.sha256()
 with path.open("rb") as f:
  for chunk in iter(lambda:f.read(1024*1024),b""):h.update(chunk)
 return h.hexdigest()
def load_manifest():
 try:
  d=json.loads(MANIFEST.read_text());return d if isinstance(d,list) else []
 except (OSError,ValueError):return []
def save_manifest(items):
 DATA.mkdir(parents=True,exist_ok=True);tmp=MANIFEST.with_suffix(".tmp");tmp.write_text(json.dumps(items,indent=2));os.replace(tmp,MANIFEST)
def within(path,root):
 try:path.resolve(strict=True).relative_to(root.resolve(strict=True));return True
 except (ValueError,OSError,RuntimeError):return False
class Handler(BaseHTTPRequestHandler):
 server_version="FileDedupLocal/2.0"
 def send_bytes(self,status,data,typ):
  self.send_response(status);self.send_header("Content-Type",typ);self.send_header("Content-Length",str(len(data)));self.send_header("X-Content-Type-Options","nosniff");self.send_header("Cache-Control","no-store");self.end_headers();self.wfile.write(data)
 def send_json(self,status,payload):self.send_bytes(status,json.dumps(payload).encode(),"application/json; charset=utf-8")
 def body(self):
  try:n=int(self.headers.get("Content-Length","0"))
  except ValueError:raise ValueError("Invalid request length.")
  if n<=0 or n>MAX_BODY:raise ValueError("Request body is empty or too large.")
  try:return json.loads(self.rfile.read(n))
  except (json.JSONDecodeError,UnicodeDecodeError):raise ValueError("Invalid JSON request.")
 def do_GET(self):
  p=urlparse(self.path).path
  if p=="/api/status":return self.send_json(200,{"ok":True,"scannerAvailable":SCANNER.is_file() and os.access(SCANNER,os.X_OK),"mode":"review-and-quarantine","quarantineEnabled":True})
  if p=="/api/quarantine":
   items=load_manifest();return self.send_json(200,{"items":[{k:x.get(k) for k in ("id","name","originalPath","quarantinePath","sizeBytes","movedAt")} for x in items if Path(x.get("quarantinePath","/nonexistent")).is_file()]})
  rel="index.html" if p=="/" else unquote(p).lstrip("/");f=(FRONTEND/rel).resolve()
  if f!=FRONTEND.resolve() and FRONTEND.resolve() not in f.parents:return self.send_json(403,{"error":"Forbidden path."})
  if not f.is_file():return self.send_json(404,{"error":"Not found."})
  types={".html":"text/html; charset=utf-8",".css":"text/css; charset=utf-8",".js":"text/javascript; charset=utf-8",".svg":"image/svg+xml"}
  try:data=f.read_bytes()
  except OSError:return self.send_json(500,{"error":"Could not read frontend file."})
  return self.send_bytes(200,data,types.get(f.suffix,"application/octet-stream"))
 def do_POST(self):
  global LAST_REPORT
  p=urlparse(self.path).path
  try:payload=self.body()
  except ValueError as e:return self.send_json(400,{"error":str(e)})
  if p=="/api/scan":
   if not isinstance(payload,dict) or not isinstance(payload.get("path"),str):return self.send_json(400,{"error":"Provide a directory path."})
   raw=payload["path"].strip()
   if not raw or "\x00" in raw:return self.send_json(400,{"error":"Enter a valid directory path."})
   target=Path(raw).expanduser()
   if not target.is_absolute():return self.send_json(400,{"error":"Use an absolute directory path."})
   try:target=target.resolve(strict=True)
   except (OSError,RuntimeError):return self.send_json(400,{"error":"Directory does not exist or cannot be accessed."})
   if not target.is_dir():return self.send_json(400,{"error":"The selected path is not a directory."})
   if not SCANNER.is_file() or not os.access(SCANNER,os.X_OK):return self.send_json(500,{"error":"C scanner is missing. Compile backend/main.c first."})
   try:r=subprocess.run([str(SCANNER),str(target)],capture_output=True,text=True,timeout=600,check=False,cwd=str(ROOT))
   except subprocess.TimeoutExpired:return self.send_json(504,{"error":"Scan timed out after 10 minutes."})
   except OSError:return self.send_json(500,{"error":"Could not start C scanner."})
   if len(r.stdout)>MAX_OUTPUT:return self.send_json(500,{"error":"Report is too large."})
   if r.returncode!=0:return self.send_json(500,{"error":(r.stderr or r.stdout or "Scanner failed.")[-2000:].strip()})
   report=parse_report(r.stdout);report["scannedPath"]=str(target);report["readOnly"]=True;report["warning"]="Cleanup is review-first and recoverable.";LAST_REPORT=report
   return self.send_json(200,report)
  if p=="/api/cleanup":
   if not isinstance(payload,dict) or not isinstance(payload.get("paths"),list) or not LAST_REPORT:return self.send_json(400,{"error":"Run a scan before requesting cleanup."})
   paths=payload["paths"]
   if not paths or len(paths)>500 or any(not isinstance(x,str) for x in paths):return self.send_json(400,{"error":"Select between 1 and 500 files."})
   review={f["path"]:(g,f) for g in LAST_REPORT["groups"] for f in g["files"] if f["role"]=="review"};moved=[];failed=[];manifest=load_manifest();qdir=QUARANTINE/ datetime.now().strftime("%Y%m%d-%H%M%S")
   for raw in dict.fromkeys(paths):
    try:
     if raw not in review:raise ValueError("This file is not a review copy from the latest scan.")
     g,f=review[raw];src=Path(raw)
     if not within(src,Path(LAST_REPORT["scannedPath"])) or not src.is_file():raise ValueError("File is missing or outside the scanned directory.")
     ref=next((Path(x["path"]) for x in g["files"] if x["role"]=="reference"),None)
     if ref is None or not ref.is_file():raise ValueError("Reference copy is missing; refusing to move this file.")
     if src.stat().st_size!=ref.stat().st_size or sha256(src)!=sha256(ref):raise ValueError("File contents no longer match the reference copy.")
     qdir.mkdir(parents=True,exist_ok=True);dest=qdir/(uuid.uuid4().hex+"__"+src.name)
     size=src.stat().st_size;digest=sha256(src);original=str(src);shutil.move(str(src),str(dest))
     item={"id":uuid.uuid4().hex,"name":src.name,"originalPath":original,"quarantinePath":str(dest),"sizeBytes":size,"sha256":digest,"movedAt":datetime.now(timezone.utc).isoformat()};manifest.append(item);moved.append({"path":original,"quarantinePath":str(dest)})
    except Exception as e:failed.append({"path":raw,"error":str(e)})
   save_manifest(manifest);return self.send_json(200,{"moved":moved,"failed":failed})
  if p=="/api/restore":
   if not isinstance(payload,dict) or not isinstance(payload.get("id"),str):return self.send_json(400,{"error":"Provide a quarantine item ID."})
   items=load_manifest();item=next((x for x in items if x.get("id")==payload["id"]),None)
   if not item:return self.send_json(404,{"error":"Quarantine item not found."})
   src=Path(item["quarantinePath"]);dest=Path(item["originalPath"])
   if not src.is_file():return self.send_json(404,{"error":"Quarantined file is missing."})
   if dest.exists():return self.send_json(409,{"error":"A file already exists at the original path. Move it safely first; nothing was overwritten."})
   try:
    if sha256(src)!=item["sha256"]:return self.send_json(409,{"error":"Quarantine file hash changed; refusing to restore."})
    dest.parent.mkdir(parents=True,exist_ok=True);shutil.move(str(src),str(dest));items.remove(item);save_manifest(items);return self.send_json(200,{"path":str(dest)})
   except OSError as e:return self.send_json(500,{"error":f"Restore failed: {e}"})
  return self.send_json(404,{"error":"Endpoint not found."})
 def log_message(self,fmt,*args):pass
if __name__=="__main__":
 if not FRONTEND.is_dir():raise SystemExit("Frontend folder is missing.")
 print(f"FileDedup dashboard: http://{HOST}:{PORT}");print("Local-only server. Cleanup moves verified review copies to a recoverable quarantine.")
 try:ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
 except KeyboardInterrupt:print("\nFileDedup server stopped.")
