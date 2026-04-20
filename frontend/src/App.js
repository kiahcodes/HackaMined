import { useState, useEffect, useRef, createContext, useContext } from "react";

// ─── API Configuration ────────────────────────────────────────────────────────
// Replace BASE_URL with your real backend URL when ready.
const BASE_URL = "http://10.21.60.40:8000";

// GET /api/city-risk
// Returns: { "Delhi": 82, "Mumbai": 76, ... }
async function apiGetCityRisk() {
  const res = await fetch(`${BASE_URL}/api/city-risk`);
  if (!res.ok) throw new Error(`city-risk: ${res.status}`);
  return res.json();
}

// GET /api/high-risk-jobs
// Returns: [ { role, city, risk }, ... ]
async function apiGetHighRiskJobs() {
  const res = await fetch(`${BASE_URL}/api/high-risk-jobs`);
  if (!res.ok) throw new Error(`high-risk-jobs: ${res.status}`);
  return res.json();
}

// POST /api/analyze-worker
// Body:    { title, city, experience, tasks }
// Returns: full worker analysis object — riskScore, wSkills, mSkills,
//          skillLabels, missing, careerNodes, etc.
async function apiAnalyzeWorker(formData) {
  const res = await fetch(`${BASE_URL}/api/analyze-worker`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title:      formData.title,
      city:       formData.city,
      experience: Number(formData.experience) || 0,
      tasks:      formData.tasks,
    }),
  });
  if (!res.ok) throw new Error(`analyze-worker: ${res.status}`);
  return res.json();
}

// POST /predict
// Body:    { name, age, gender, department, experience }
// Returns: { risk_score: 0.72, message: "Prediction generated successfully" }
async function apiPredict(profileData) {
  const res = await fetch(`${BASE_URL}/api/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name:       profileData.name       || "",
      age:        Number(profileData.age)        || 0,
      gender:     profileData.gender     || "",
      department: profileData.department || "",
      experience: Number(profileData.experience) || 0,
    }),
  });
  if (!res.ok) throw new Error(`predict: ${res.status}`);
  return res.json(); // { risk_score, message }
}

// POST /api/course-recommendations
// Body:    { title, city, skills } — pass worker profile context
// Returns: [ { name, provider, duration, skill, url, month?, free? }, ... ]
async function apiGetCourseRecommendations(workerContext) {
  const res = await fetch(`${BASE_URL}/api/course-recommendations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(workerContext),
  });
  if (!res.ok) throw new Error(`course-recommendations: ${res.status}`);
  return res.json();
}

// GET /api/job-impact?ai_level=60
// Returns: { bpoRemaining, aiGrowth, userRisk }
async function apiGetJobImpact(aiLevel) {
  const res = await fetch(`${BASE_URL}/api/job-impact?ai_level=${aiLevel}`);
  if (!res.ok) throw new Error(`job-impact: ${res.status}`);
  return res.json(); // { bpoRemaining, aiGrowth, userRisk }
}

// GET /api/stats
// Returns: { total_jobs, total_cities, total_skills, total_ai_signals }
async function apiGetStats() {
  const res = await fetch(`${BASE_URL}/api/stats`);
  if (!res.ok) throw new Error(`stats: ${res.status}`);
  return res.json();
}

// ─── Theme Context ────────────────────────────────────────────────────────────

const ThemeContext = createContext({ dark: true, toggle: () => {} });
function useTheme() { return useContext(ThemeContext); }

function useT() {
  const { dark } = useTheme();
  return {
    bg:         dark ? "#0B1220"  : "#F8FAFC",
    bgSidebar:  dark ? "#0c1424"  : "#F1F5F9",
    bgHeader:   dark ? "rgba(11,18,32,0.93)" : "rgba(248,250,252,0.95)",
    bgInput:    dark ? "#1F2937"  : "#F1F5F9",
    bgInputBdr: dark ? "rgba(255,255,255,0.14)" : "rgba(0,0,0,0.14)",
    bgCard:     dark
      ? "linear-gradient(135deg,rgba(255,255,255,0.06) 0%,rgba(255,255,255,0.02) 100%)"
      : "linear-gradient(135deg,rgba(0,0,0,0.025) 0%,rgba(0,0,0,0.01) 100%)",
    border:     dark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.09)",
    borderHov:  dark ? "rgba(59,130,246,0.5)"  : "rgba(59,130,246,0.55)",
    textPri:    dark ? "#F8FAFC"  : "#111827",
    textSec:    dark ? "#CBD5E1"  : "#374151",
    textMuted:  dark ? "#64748b"  : "#9CA3AF",
    gridStroke: dark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.08)",
    radarLbl:   dark ? "rgba(255,255,255,0.6)"  : "rgba(0,0,0,0.55)",
    gaugeScore: dark ? "#F8FAFC"  : "#111827",
    gaugeSub:   dark ? "rgba(255,255,255,0.45)" : "rgba(0,0,0,0.4)",
    scrollBar:  dark ? "#1e3a5f"  : "#CBD5E1",
    chatBg:     dark ? "#0f1729"  : "#FFFFFF",
    chatBot:    dark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.06)",
    chatBotTxt: dark ? "#e2e8f0"  : "#1e293b",
  };
}

// ─── Data ─────────────────────────────────────────────────────────────────────

const CITIES = ["Delhi","Mumbai","Bangalore","Hyderabad","Chennai","Kolkata","Pune",
  "Ahmedabad","Surat","Jaipur","Lucknow","Chandigarh","Indore","Bhopal",
  "Coimbatore","Kochi","Nagpur","Visakhapatnam","Vadodara","Patna","Guwahati","Mysuru"];

const SECTORS = ["IT & Software","BPO / Call Center","Manufacturing","Healthcare",
  "Finance & Banking","Retail & E-Commerce","Logistics","Education","Construction","Agriculture"];

const TICKER = [
  "🔥 AI Engineer hiring in Ahmedabad grew 82% in the last 30 days.",
  "📉 Customer Support hiring in Indore dropped 21% this quarter.",
  "⚠️ Automation mentions in BPO job postings increased 35% nationwide.",
  "📈 Data Analyst demand increased 47% in Bangalore — highest in 2 years.",
  "🤖 67% of new IT job postings now require at least one AI tool skill.",
  "💼 Cloud Architect openings in Hyderabad up 55% vs last month.",
  "🚨 Manual Data Entry roles declined 72% in Delhi over 12 months.",
  "✅ Customer Success Manager roles grew 44% across Tier-2 cities.",
  "📊 Fintech hiring in Mumbai surged 38% — 3rd consecutive quarter of growth.",
  "🌐 Remote AI roles from Indian talent pools up 91% globally.",
];

const RISING = [
  { skill:"Generative AI Prompting", demand:96, change:84 },
  { skill:"LLM Fine-tuning", demand:91, change:71 },
  { skill:"MLOps & AI Infrastructure", demand:88, change:65 },
  { skill:"Cybersecurity Analytics", demand:85, change:58 },
  { skill:"Data Engineering (dbt/Airflow)", demand:83, change:52 },
  { skill:"Cloud Architecture (AWS/GCP)", demand:82, change:48 },
  { skill:"React / Next.js", demand:79, change:41 },
  { skill:"DevSecOps", demand:76, change:38 },
  { skill:"Customer Success Analytics", demand:74, change:35 },
  { skill:"AI-Assisted QA Testing", demand:71, change:33 },
  { skill:"Product Analytics", demand:68, change:29 },
  { skill:"Blockchain Development", demand:65, change:27 },
  { skill:"Edge Computing", demand:62, change:24 },
  { skill:"Robotic Process Automation", demand:60, change:22 },
  { skill:"Healthcare Data Analytics", demand:58, change:21 },
  { skill:"Video Production & Editing", demand:55, change:18 },
  { skill:"Supply Chain AI", demand:53, change:17 },
  { skill:"Fintech Compliance", demand:51, change:15 },
  { skill:"Green Energy Analytics", demand:49, change:14 },
  { skill:"AR/VR Development", demand:47, change:12 },
];

const DECLINING = [
  { skill:"Manual Data Entry", demand:12, change:72 },
  { skill:"Basic Excel Reporting", demand:18, change:65 },
  { skill:"Telemarketing Scripts", demand:15, change:61 },
  { skill:"Cold Call Operations", demand:14, change:58 },
  { skill:"Manual QA Testing", demand:22, change:54 },
  { skill:"Traditional SEO (keyword stuffing)", demand:25, change:49 },
  { skill:"Legacy COBOL Programming", demand:19, change:47 },
  { skill:"Physical Cashier Operations", demand:28, change:44 },
  { skill:"Basic Customer Support (L1)", demand:31, change:42 },
  { skill:"Print Media Design", demand:33, change:38 },
  { skill:"Manual Bookkeeping", demand:29, change:36 },
  { skill:"Traditional Retail Management", demand:35, change:34 },
  { skill:"Basic Coding (Copy-Paste)", demand:38, change:31 },
  { skill:"Fax & Postal Services", demand:8, change:88 },
  { skill:"Film Photography Development", demand:6, change:91 },
  { skill:"Physical Inventory Counting", demand:24, change:52 },
  { skill:"Manual Scheduling", demand:27, change:45 },
  { skill:"Basic Help Desk (Password Reset)", demand:32, change:39 },
  { skill:"Typewriting Services", demand:5, change:94 },
  { skill:"Manual Payroll Processing", demand:21, change:61 },
];

const CITY_RISK = {
  Delhi:68, Mumbai:61, Bangalore:44, Hyderabad:47, Chennai:52, Kolkata:71,
  Pune:49, Ahmedabad:63, Surat:66, Jaipur:58, Lucknow:74, Chandigarh:55,
  Indore:69, Bhopal:72, Coimbatore:57, Kochi:51, Nagpur:67,
  Visakhapatnam:65, Vadodara:62, Patna:78, Guwahati:76, Mysuru:54,
};

const HIRING_DATA = {
  Delhi:[42,45,48,43,51,58,62,67,65,72,78,85],
  Mumbai:[55,58,61,57,63,68,71,75,73,79,84,91],
  Bangalore:[88,92,95,90,98,105,112,118,115,122,129,138],
  Hyderabad:[71,74,78,73,80,87,92,97,94,101,108,115],
  Chennai:[48,51,54,49,57,63,68,73,70,77,83,89],
  default:[30,32,35,31,38,43,46,51,48,55,61,67],
};

const SECTORS_DATA = {
  "IT & Software":   [120,135,142,138,155,168,175,182,179,192,205,218],
  "BPO / Call Center":[88,82,78,85,74,68,62,71,65,58,52,47],
  "Healthcare":      [45,48,52,49,56,61,65,70,67,74,80,87],
  "Finance & Banking":[67,71,75,70,78,84,89,94,91,98,104,111],
  "Manufacturing":   [52,49,46,53,44,41,38,45,42,39,36,33],
};

// Hiring trend data broken down by sector per city (Feature 1)
const HIRING_BY_SECTOR = {
  "All Sectors": HIRING_DATA,
  "IT & Software": {
    Delhi:[48,52,55,50,58,65,71,77,74,82,89,97], Mumbai:[62,66,70,65,73,80,86,92,89,97,104,113],
    Bangalore:[95,102,108,104,115,124,132,140,137,148,158,170], Hyderabad:[78,83,88,82,91,99,106,113,110,119,128,138],
    Chennai:[52,56,60,55,63,70,76,83,79,87,94,103], default:[35,38,41,37,44,51,57,63,60,68,75,83],
  },
  "BPO / Call Center": {
    Delhi:[55,50,46,52,43,38,33,40,36,30,26,22], Mumbai:[60,55,51,57,48,43,38,45,41,35,30,26],
    Bangalore:[71,65,60,68,57,51,45,53,48,41,36,31], Hyderabad:[48,44,40,46,38,33,29,35,31,26,22,18],
    Chennai:[42,38,35,40,33,29,25,30,27,22,19,16], default:[40,36,33,38,30,26,22,28,24,19,15,12],
  },
  "Healthcare": {
    Delhi:[38,41,44,40,47,52,57,62,59,66,72,79], Mumbai:[44,47,51,47,54,60,65,71,68,75,81,88],
    Bangalore:[42,45,49,46,53,58,63,69,66,73,80,88], default:[28,30,33,30,36,40,44,49,46,52,58,64],
  },
  "Finance & Banking": {
    Mumbai:[72,77,82,76,85,93,99,106,102,111,119,128], Bangalore:[58,62,66,61,69,76,82,88,85,93,100,108],
    Delhi:[51,55,59,54,62,69,74,80,77,84,91,99], default:[44,47,51,47,54,60,65,71,68,75,81,88],
  },
  "Manufacturing": {
    default:[48,45,42,49,40,37,34,41,38,34,31,28],
  },
  "Retail & E-Commerce": {
    Mumbai:[55,58,62,57,65,71,77,83,80,88,95,103], Delhi:[49,52,56,51,58,64,70,76,73,80,87,95],
    default:[38,41,44,40,47,52,57,62,59,66,72,79],
  },
  "Logistics": { default:[33,35,38,34,40,45,49,54,51,57,63,70] },
  "Education":  { default:[28,30,32,29,34,38,42,46,44,49,54,60] },
  "Construction":{ default:[22,24,26,23,28,32,35,39,37,41,46,51] },
  "Agriculture": { default:[18,19,21,19,23,26,29,32,30,34,38,43] },
};

// Early Warning watchlist data (Feature 4)
const EARLY_WARNING = [
  { category:"BPO / Call Center",   declineRate:38, affectedCities:14, severity:"critical", msg:"L1 support roles being replaced by AI agents. Workers should pivot to AI-assisted CS roles." },
  { category:"Data Entry & Clerical",declineRate:72, affectedCities:22, severity:"critical", msg:"Automation covers 90%+ of tasks. Immediate reskilling to data analysis recommended." },
  { category:"Physical Retail",     declineRate:44, affectedCities:18, severity:"high",     msg:"E-commerce and self-checkout displacing floor staff. Digital sales skills needed." },
  { category:"Manual Manufacturing", declineRate:31, affectedCities:12, severity:"high",     msg:"Robotic process automation reducing assembly line headcount in Tier-1 cities." },
  { category:"Basic Accounting",    declineRate:29, affectedCities:9,  severity:"medium",   msg:"AI bookkeeping tools handling routine entries. Upskill to financial analytics." },
  { category:"Traditional Marketing",declineRate:25, affectedCities:7,  severity:"medium",   msg:"AI content generation reducing demand for basic copy roles. Learn AI-assisted marketing." },
];

// City-specific job risk rankings (Feature 3)
const CITY_JOB_RANKINGS = {
  Delhi:     { highest:[{role:"Data Entry Clerk",risk:94},{role:"Call Center Agent",risk:88},{role:"Cashier",risk:85},{role:"Telemarketer",risk:82},{role:"Basic Accountant",risk:79}], lowest:[{role:"AI Policy Analyst",risk:12},{role:"Cybersecurity Specialist",risk:16},{role:"Urban Planner",risk:21},{role:"Data Scientist",risk:24},{role:"Cloud Architect",risk:27}] },
  Mumbai:    { highest:[{role:"Bank Teller",risk:91},{role:"Manual Stock Trader",risk:87},{role:"Basic Accountant",risk:83},{role:"Telemarketer",risk:80},{role:"Data Entry Clerk",risk:77}], lowest:[{role:"Fintech Developer",risk:14},{role:"AI Compliance Officer",risk:18},{role:"UX Designer",risk:22},{role:"Investment Analyst (AI)",risk:25},{role:"Risk Modeling Analyst",risk:29}] },
  Bangalore: { highest:[{role:"Manual QA Tester",risk:86},{role:"Legacy COBOL Dev",risk:82},{role:"Basic IT Support",risk:78},{role:"Data Entry Clerk",risk:75},{role:"Basic Help Desk",risk:71}], lowest:[{role:"AI/ML Engineer",risk:11},{role:"AI Research Scientist",risk:13},{role:"DevOps Lead",risk:15},{role:"Product Manager (AI)",risk:19},{role:"ML Platform Engineer",risk:22}] },
  Hyderabad: { highest:[{role:"BPO Executive L1",risk:89},{role:"Data Entry Clerk",risk:85},{role:"Telemarketer",risk:81},{role:"Manual Tester",risk:77},{role:"Basic Admin",risk:74}], lowest:[{role:"Cloud Architect",risk:13},{role:"AI Engineer",risk:16},{role:"DevOps Engineer",risk:20},{role:"Data Scientist",risk:23},{role:"Product Analyst",risk:27}] },
  Chennai:   { highest:[{role:"BPO Agent",risk:87},{role:"Data Entry",risk:83},{role:"Manual Tester",risk:79},{role:"Telemarketer",risk:76},{role:"Basic Accountant",risk:72}], lowest:[{role:"Full Stack Dev",risk:15},{role:"AI Engineer",risk:18},{role:"Cloud Engineer",risk:21},{role:"UX Researcher",risk:25},{role:"Data Analyst",risk:28}] },
  default:   { highest:[{role:"Data Entry Clerk",risk:92},{role:"Telemarketer",risk:88},{role:"BPO Executive L1",risk:85},{role:"Manual QA Tester",risk:81},{role:"Cashier",risk:78}], lowest:[{role:"AI Specialist",risk:15},{role:"Cybersecurity Analyst",risk:19},{role:"Data Analyst",risk:20},{role:"Cloud Engineer",risk:24},{role:"Digital Marketer",risk:28}] },
};

// Employer supply/demand gap data (Feature 5)
const EMPLOYER_SUPPLY_DEMAND = {
  "IT & Software":    [{skill:"Generative AI Prompting",supply:18,demand:96},{skill:"LLM Fine-tuning",supply:12,demand:91},{skill:"MLOps & Infrastructure",supply:22,demand:88},{skill:"Cybersecurity Analytics",supply:31,demand:85},{skill:"Data Engineering",supply:38,demand:83},{skill:"Cloud Architecture",supply:44,demand:82},{skill:"React / Next.js",supply:55,demand:79},{skill:"DevSecOps",supply:41,demand:76}],
  "BPO / Call Center":[{skill:"AI Support Tools",supply:15,demand:71},{skill:"Customer Success Analytics",supply:29,demand:74},{skill:"CRM Automation",supply:33,demand:68},{skill:"Sentiment Analysis",supply:21,demand:65},{skill:"Multilingual AI Tools",supply:18,demand:62}],
  "Healthcare":       [{skill:"Healthcare Data Analytics",supply:20,demand:58},{skill:"Medical Coding AI",supply:17,demand:54},{skill:"Telehealth Platforms",supply:28,demand:51},{skill:"EHR Systems",supply:35,demand:48}],
  "Finance & Banking":[{skill:"Fintech Compliance",supply:33,demand:51},{skill:"Algorithmic Trading",supply:19,demand:63},{skill:"Risk Modeling AI",supply:24,demand:59},{skill:"Fraud Detection ML",supply:21,demand:67}],
  "Manufacturing":    [{skill:"Robotics Programming",supply:14,demand:52},{skill:"Industrial IoT",supply:18,demand:47},{skill:"Supply Chain AI",supply:22,demand:53},{skill:"Quality Automation",supply:19,demand:44}],
};

const COURSES = [
  { month:1, name:"NPTEL Data Analytics Basics",       duration:"8 weeks",  skill:"Data Literacy",     free:true,  provider:"NPTEL",    url:"https://nptel.ac.in/courses/106105174" },
  { month:2, name:"SWAYAM AI Fundamentals",            duration:"6 weeks",  skill:"AI Literacy",       free:true,  provider:"SWAYAM",   url:"https://onlinecourses.nptel.ac.in/noc22_cs44" },
  { month:3, name:"PMKVY Digital Marketing Course",    duration:"45 days",  skill:"Digital Marketing", free:true,  provider:"PMKVY",    url:"https://www.pmkvyofficial.org/" },
  { month:4, name:"Google Data Analytics Certificate", duration:"6 months", skill:"Data Analytics",    free:false, provider:"Coursera", url:"https://www.coursera.org/professional-certificates/google-data-analytics" },
  { month:5, name:"IBM AI Engineering Certificate",    duration:"8 months", skill:"AI Engineering",    free:false, provider:"Coursera", url:"https://www.coursera.org/professional-certificates/ai-engineer" },
  { month:6, name:"LinkedIn: Customer Success Mgmt",   duration:"4 weeks",  skill:"CS Strategy",       free:false, provider:"LinkedIn", url:"https://www.linkedin.com/learning/" },
];

const BOT_RESP = {
  risk: {
    en:"Your risk score of **74/100** is HIGH primarily because:\n\n• **Hiring Decline**: BPO sector jobs dropped **38%** in 90 days\n• **AI Tool Mentions**: 67% of similar postings mention automation tools\n• **Skill Mismatch**: Your skills overlap with tasks LLMs now do at 80%+ accuracy\n\nWorkers who upskill see risk scores drop 40–50 points within 6 months.",
    hi:"आपका जोखिम स्कोर **74/100** HIGH है क्योंकि:\n\n• **नौकरी में गिरावट**: BPO नौकरियां **38%** कम हुई हैं\n• **AI उपकरण**: 67% पोस्टिंग में AI टूल्स का उल्लेख है\n• **कौशल अंतर**: आपके कौशल AI से प्रभावित हैं\n\nAI-सहायक भूमिकाओं में कौशल विकास से 6 महीने में जोखिम 40–50 अंक कम होता है।",
  },
  safer: {
    en:"Based on your profile, here are **5 lower-risk transitions**:\n\n1. **AI Customer Experience Analyst** — Risk: 18 | Salary: +35%\n2. **Customer Success Manager** — Risk: 28 | Salary: +22%\n3. **Operations Analytics Specialist** — Risk: 31 | Salary: +18%\n4. **Digital Marketing Analyst** — Risk: 24 | Salary: +28%\n5. **HR Technology Consultant** — Risk: 19 | Salary: +31%",
    hi:"आपकी प्रोफ़ाइल के आधार पर **5 कम जोखिम वाले करियर**:\n\n1. **AI CX Analyst** — जोखिम: 18 | वेतन: +35%\n2. **Customer Success Manager** — जोखिम: 28 | वेतन: +22%\n3. **Operations Analytics** — जोखिम: 31 | वेतन: +18%\n4. **Digital Marketing Analyst** — जोखिम: 24 | वेतन: +28%\n5. **HR Tech Consultant** — जोखिम: 19 | वेतन: +31%",
  },
  "3month": {
    en:"**Quick 3-month upskilling paths**:\n\n**Month 1** → NPTEL: Data Analytics Basics (Free, 8 weeks)\n**Month 2** → SWAYAM: AI Fundamentals (Free, 6 weeks)\n**Month 3** → PMKVY: Digital Marketing (Free, 45 days)\n\nBonus: PMKVY centres offer **free 45-day AI Literacy** cert — reduces risk by 15–20 points.",
    hi:"**3 महीने के कौशल विकास पाठ्यक्रम**:\n\n**माह 1** → NPTEL: डेटा एनालिटिक्स (निःशुल्क)\n**माह 2** → SWAYAM: AI फंडामेंटल्स (निःशुल्क)\n**माह 3** → PMKVY: डिजिटल मार्केटिंग (निःशुल्क)",
  },
  bpo: {
    en:"**BPO Job Market — Indore (Live Signals)**\n\n• Active listings: **1,247 jobs**\n• Trend: ↓ 31% vs last quarter\n• Avg salary: ₹22,000–₹34,000/month\n• Top hiring: Wipro BPO, Infosys BPM, Tech Mahindra\n\n⚠️ 42% of new postings require AI tool proficiency. L1 support roles down 58% YoY.",
    hi:"**BPO नौकरी बाजार — इंदौर**\n\n• सक्रिय पोस्टिंग: **1,247 नौकरियां**\n• रुझान: ↓ 31% पिछली तिमाही से\n• वेतन: ₹22,000–₹34,000/माह\n\n⚠️ 42% नई पोस्टिंग में AI टूल्स की आवश्यकता है।",
  },
  hindi: {
    en:"",
    hi:"नमस्ते! मैं आपका **AI करियर सलाहकार** हूं।\n\n🎯 **तत्काल**: AI टूल्स सीखें\n📚 **3 महीने में**: SWAYAM पर डेटा एनालिटिक्स\n💼 **6 महीने में**: AI-सहायक भूमिका में संक्रमण\n\nआपका जोखिम **74** से **35** तक लाया जा सकता है।",
  },
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

const riskColor = s => s <= 33 ? "#22c55e" : s <= 66 ? "#f59e0b" : "#ef4444";
const riskLabel = s => s <= 33 ? "LOW RISK" : s <= 66 ? "MEDIUM RISK" : "HIGH RISK";

function useCounter(target, ms = 1400) {
  const [v, setV] = useState(0);
  useEffect(() => {
    let cur = 0;
    const step = target / (ms / 16);
    const t = setInterval(() => {
      cur += step;
      if (cur >= target) { setV(target); clearInterval(t); }
      else setV(Math.floor(cur));
    }, 16);
    return () => clearInterval(t);
  }, [target]);
  return v;
}

// ─── Atoms ────────────────────────────────────────────────────────────────────

function Dot({ color = "#3b82f6" }) {
  return (
    <span className="relative inline-flex w-2 h-2 flex-shrink-0">
      <span className="absolute inset-0 rounded-full opacity-75 animate-ping" style={{ background: color }} />
      <span className="relative rounded-full w-2 h-2" style={{ background: color }} />
    </span>
  );
}

function Pill({ children, color = "blue" }) {
  const map = {
    blue:   "bg-blue-500/20 text-blue-400 border-blue-500/30",
    orange: "bg-orange-500/20 text-orange-400 border-orange-500/30",
    green:  "bg-green-500/20 text-green-400 border-green-500/30",
    red:    "bg-red-500/20 text-red-400 border-red-500/30",
    yellow: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
    purple: "bg-purple-500/20 text-purple-400 border-purple-500/30",
  };
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border font-medium whitespace-nowrap ${map[color] || map.blue}`}>
      {children}
    </span>
  );
}

function Card({ children, className = "", hover = true, onClick, xStyle }) {
  const t = useT();
  const [hov, setHov] = useState(false);
  return (
    <div
      onClick={onClick}
      className={`rounded-2xl backdrop-blur-sm shadow-xl transition-all duration-300 ${hover ? "hover:-translate-y-0.5" : ""} ${onClick ? "cursor-pointer" : ""} ${className}`}
      style={{
        background: t.bgCard,
        border: `1px solid ${hov && hover ? t.borderHov : t.border}`,
        ...xStyle,
      }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
    >
      {children}
    </div>
  );
}

// StyledInput — fixes input text visibility in both themes
function SI({ as = "input", xStyle = {}, className = "", ...props }) {
  const t = useT();
  const [focused, setFocused] = useState(false);
  const base = {
    background: t.bgInput,
    color: t.textPri,
    border: `1.5px solid ${focused ? "#3b82f6" : t.bgInputBdr}`,
    ...xStyle,
  };
  const cls = `w-full rounded-xl px-4 py-3 text-sm outline-none transition-all ${className}`;
  const handlers = {
    onFocus: () => setFocused(true),
    onBlur:  () => setFocused(false),
  };
  if (as === "textarea") return <textarea className={cls} style={base} {...handlers} {...props} />;
  if (as === "select")   return <select   className={cls} style={base} {...handlers} {...props} />;
  return <input className={cls} style={base} {...handlers} {...props} />;
}

// ─── Theme Toggle ─────────────────────────────────────────────────────────────

function ThemeToggle() {
  const { dark, toggle } = useTheme();
  const t = useT();
  return (
    <button
      onClick={toggle}
      className="flex items-center gap-2 rounded-xl px-3 py-1.5 text-xs font-semibold transition-all border"
      style={{ background: dark ? "rgba(255,255,255,0.05)" : "rgba(0,0,0,0.05)", borderColor: t.border, color: t.textSec }}
    >
      <span>{dark ? "☀️" : "🌙"}</span>
      <span>{dark ? "Light" : "Dark"}</span>
      <span className="relative inline-flex items-center w-8 h-4 rounded-full ml-1 transition-colors"
        style={{ background: dark ? "#3b82f6" : "#CBD5E1" }}>
        <span className="absolute w-3 h-3 bg-white rounded-full shadow transition-all duration-300"
          style={{ left: dark ? "17px" : "2px" }} />
      </span>
    </button>
  );
}

// ─── News Ticker ──────────────────────────────────────────────────────────────

function Ticker() {
  const t = useT();
  const { dark } = useTheme();
  const doubled = [...TICKER, ...TICKER];
  return (
    <div className="rounded-xl overflow-hidden flex items-stretch"
      style={{
        background: dark
          ? "linear-gradient(90deg,rgba(59,130,246,0.08),rgba(37,99,235,0.05))"
          : "linear-gradient(90deg,rgba(59,130,246,0.06),rgba(37,99,235,0.03))",
        border: `1px solid ${dark ? "rgba(59,130,246,0.2)" : "rgba(59,130,246,0.18)"}`,
        minHeight: "38px",
      }}>
      <div className="flex-shrink-0 flex items-center gap-1.5 px-3"
        style={{ borderRight: `1px solid ${dark ? "rgba(59,130,246,0.2)" : "rgba(59,130,246,0.15)"}` }}>
        <Dot color="#3b82f6" />
        <span className="text-xs font-bold text-blue-400 whitespace-nowrap">LIVE INSIGHTS</span>
      </div>
      <div className="flex-1 overflow-hidden flex items-center py-2"
        style={{ maskImage: "linear-gradient(90deg,transparent,black 5%,black 95%,transparent)" }}>
        <div style={{
          display: "inline-flex", gap: "3rem", whiteSpace: "nowrap",
          animation: "ticker 45s linear infinite",
        }}>
          {doubled.map((item, i) => (
            <span key={i} className="text-xs font-medium flex-shrink-0" style={{ color: t.textSec }}>{item}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Charts ───────────────────────────────────────────────────────────────────

function MiniBar({ data, color = "#3b82f6", height = 40 }) {
  const mx = Math.max(...data);
  return (
    <div className="flex items-end gap-0.5" style={{ height }}>
      {data.map((v, i) => (
        <div key={i} className="flex-1 rounded-sm"
          style={{ height: `${(v / mx) * 100}%`, background: color, opacity: 0.5 + (i / data.length) * 0.5 }} />
      ))}
    </div>
  );
}

const MONTHS_SHORT = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

function LineSVG({ data, color = "#3b82f6", w = 300, h = 120, showLabels = false, skillName = "" }) {
  const [tooltip, setTooltip] = useState(null);
  const mx = Math.max(...data), mn = Math.min(...data), rng = mx - mn || 1;
  const pts = data.map((v, i) => ({
    x: (i / (data.length - 1)) * (w - 20) + 10,
    y: h - 20 - ((v - mn) / rng) * (h - 30),
    v, i,
  }));
  const path = pts.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");
  const area = `${path} L ${pts[pts.length - 1].x} ${h - 20} L ${pts[0].x} ${h - 20} Z`;
  const uid = color.replace("#", "");
  return (
    <svg width="100%" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ overflow: "visible" }}>
      <defs>
        <linearGradient id={`g${uid}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={area} fill={`url(#g${uid})`} />
      <path d={path} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      {pts.map((p, i) => (
        <g key={i}>
          <circle cx={p.x} cy={p.y} r="3" fill={color} opacity="0.85" />
          {showLabels && (
            <circle cx={p.x} cy={p.y} r="10" fill="transparent" style={{ cursor: "crosshair" }}
              onMouseEnter={() => setTooltip({ x: p.x, y: p.y, v: p.v, i: p.i })}
              onMouseLeave={() => setTooltip(null)} />
          )}
        </g>
      ))}
      {showLabels && tooltip && (() => {
        const flip = tooltip.x > w * 0.72;
        const tx = flip ? tooltip.x - 70 : tooltip.x + 6;
        const ty = tooltip.y - 30;
        return (
          <g style={{ pointerEvents: "none" }}>
            <rect x={tx} y={ty} width="64" height="22" rx="4" fill={color} fillOpacity="0.93" />
            <text x={tx + 32} y={ty + 8} textAnchor="middle" dominantBaseline="middle" fill="white" fontSize="7" fontWeight="700">
              {MONTHS_SHORT[tooltip.i]}: {tooltip.v.toFixed(0)}
            </text>
            {skillName && (
              <text x={tx + 32} y={ty + 17} textAnchor="middle" dominantBaseline="middle" fill="rgba(255,255,255,0.75)" fontSize="6">
                {skillName.length > 14 ? skillName.slice(0, 13) + "…" : skillName}
              </text>
            )}
          </g>
        );
      })()}
    </svg>
  );
}

function Gauge({ score, size = 200 }) {
  const t = useT();
  const [anim, setAnim] = useState(0);
  useEffect(() => {
    let v = 0;
    const tm = setInterval(() => {
      v += 2;
      if (v >= score) { setAnim(score); clearInterval(tm); }
      else setAnim(v);
    }, 20);
    return () => clearInterval(tm);
  }, [score]);
  const r = size * 0.38, cx = size / 2, cy = size * 0.55;
  const sa = -210 * (Math.PI / 180), ea = 30 * (Math.PI / 180);
  const na = sa + (anim / 100) * (ea - sa);
  const arc = (s, e, rad) => {
    const x1 = cx + rad * Math.cos(s), y1 = cy + rad * Math.sin(s);
    const x2 = cx + rad * Math.cos(e), y2 = cy + rad * Math.sin(e);
    return `M ${x1} ${y1} A ${rad} ${rad} 0 ${e - s > Math.PI ? 1 : 0} 1 ${x2} ${y2}`;
  };
  const c = riskColor(anim);
  return (
    <svg width={size} height={size * 0.8} viewBox={`0 0 ${size} ${size * 0.8}`}>
      <path d={arc(sa, ea, r)} fill="none" stroke="rgba(128,128,128,0.15)" strokeWidth="14" strokeLinecap="round" />
      {anim > 0 && <path d={arc(sa, na, r)} fill="none" stroke={c} strokeWidth="14" strokeLinecap="round"
        style={{ filter: `drop-shadow(0 0 8px ${c})` }} />}
      <circle cx={cx} cy={cy} r="5" fill={c} style={{ filter: `drop-shadow(0 0 6px ${c})` }} />
      <text x={cx} y={cy - 8} textAnchor="middle" fill={t.gaugeScore} fontSize={size * 0.18} fontWeight="800" fontFamily="monospace">{anim}</text>
      <text x={cx} y={cy + 12} textAnchor="middle" fill={t.gaugeSub} fontSize={size * 0.065}>/100</text>
    </svg>
  );
}

function Radar({ workerData, marketData, labels, size = 240 }) {
  const t = useT();
  const cx = size / 2, cy = size / 2, r = size * 0.37;
  const n = labels.length, step = (2 * Math.PI) / n;
  const pt = (v, i) => { const a = i * step - Math.PI / 2; return { x: cx + r * (v / 100) * Math.cos(a), y: cy + r * (v / 100) * Math.sin(a) }; };
  const toP = pts => pts.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ") + " Z";
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      {[0.25, 0.5, 0.75, 1].map(f => (
        <polygon key={f}
          points={labels.map((_, i) => { const a = i * step - Math.PI / 2; return `${cx + r * f * Math.cos(a)},${cy + r * f * Math.sin(a)}`; }).join(" ")}
          fill="none" stroke={t.gridStroke} strokeWidth="1" />
      ))}
      {labels.map((_, i) => { const a = i * step - Math.PI / 2; return <line key={i} x1={cx} y1={cy} x2={cx + r * Math.cos(a)} y2={cy + r * Math.sin(a)} stroke={t.gridStroke} />; })}
      <path d={toP(marketData.map((v, i) => pt(v, i)))} fill="rgba(59,130,246,0.15)" stroke="#3b82f6" strokeWidth="1.5" />
      <path d={toP(workerData.map((v, i) => pt(v, i)))} fill="rgba(249,115,22,0.2)" stroke="#f97316" strokeWidth="2" />
      {labels.map((lbl, i) => {
        const a = i * step - Math.PI / 2;
        const lx = cx + (r + 22) * Math.cos(a), ly = cy + (r + 22) * Math.sin(a);
        return <text key={i} x={lx} y={ly} textAnchor="middle" dominantBaseline="middle" fill={t.radarLbl} fontSize="9">{lbl}</text>;
      })}
    </svg>
  );
}

function HeatCell({ value }) {
  const bg = value >= 80 ? "rgba(239,68,68,0.7)" : value >= 60 ? "rgba(249,115,22,0.6)" : value >= 40 ? "rgba(234,179,8,0.5)" : "rgba(34,197,94,0.5)";
  return <div className="rounded text-center text-xs font-bold py-1 text-white" style={{ background: bg }}>{value}</div>;
}

// ─── Career Node ──────────────────────────────────────────────────────────────

function CareerNode({ role, risk, salary, skills, isFirst }) {
  const t = useT();
  const [hov, setHov] = useState(false);
  const c = riskColor(risk);
  return (
    <div className="flex flex-col items-center">
      <div className="relative rounded-2xl border-2 p-4 w-56 text-center transition-all duration-300 cursor-pointer"
        style={{ borderColor: hov ? c : `${c}60`, background: hov ? `${c}18` : t.bgCard, transform: hov ? "scale(1.04)" : "scale(1)", boxShadow: hov ? `0 0 20px ${c}40` : "none" }}
        onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}>
        {isFirst && <div className="absolute -top-3 left-1/2 -translate-x-1/2 text-xs px-2 py-0.5 rounded-full bg-orange-500 text-white font-bold whitespace-nowrap">CURRENT</div>}
        <div className="text-sm font-bold mb-1" style={{ color: t.textPri }}>{role}</div>
        <div className="text-xs font-black mb-2" style={{ color: c }}>Risk: {risk}/100</div>
        <div className="text-xs text-green-500 mb-2">{salary}</div>
        <div className="flex flex-wrap gap-1 justify-center">
          {skills.map(s => <span key={s} className="text-xs px-1.5 py-0.5 rounded" style={{ background: "rgba(128,128,128,0.15)", color: t.textSec }}>{s}</span>)}
        </div>
      </div>
      <div className="flex flex-col items-center my-1" style={{ color: t.textMuted }}>
        <div className="w-px h-4" style={{ background: t.border }} /><div className="text-xs">↓</div><div className="w-px h-4" style={{ background: t.border }} />
      </div>
    </div>
  );
}

// ─── Chatbot ──────────────────────────────────────────────────────────────────

function Chatbot({ workerProfile: propProfile }) {
  const t = useT();
  const { dark } = useTheme();

  // Use prop profile if available, otherwise generic fallback
  const workerProfile = propProfile || {
    job_title: "Worker",
    city: "India",
    years_experience: 0,
    write_up: "Not yet specified.",
    risk_score: 50,
  };

  const [open, setOpen] = useState(false);
  const [lang, setLang] = useState("en");
  const [msgs, setMsgs] = useState([{ role: "bot", text: "Ask about your risk score, safer careers, or learning paths." }]);
  const [inp, setInp] = useState("");
  const [thinking, setThinking] = useState(false);
  const [focused, setFocused] = useState(false);
  const [sessionReady, setSessionReady] = useState(false);
  const [sessionId, setSessionId] = useState(() => "session_" + Date.now());
  const prevTitleRef = useRef(null);
  const endRef = useRef(null);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [msgs]);

  // Reset session when job title changes so bot forgets old identity
  useEffect(() => {
    if (workerProfile.job_title !== prevTitleRef.current) {
      prevTitleRef.current = workerProfile.job_title;
      if (prevTitleRef.current !== null) {
        setSessionId("session_" + Date.now());
        setSessionReady(false);
      }
    }
  }, [workerProfile.job_title]);

  // Create session when chat opens
  useEffect(() => {
    if (!open || sessionReady) return;
    fetch(`${BASE_URL}/chat/session`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, worker_profile: workerProfile }),
    })
      .then(() => setSessionReady(true))
      .catch(() => setSessionReady(true)); // still allow chat even if session fails
  }, [open]);

  const QUICK = [
    { label: "Why is my risk score high?", text: "Why is my risk score high?" },
    { label: "What jobs are safer for me?", text: "What jobs are safer for me?" },
    { label: "Show me 3-month learning paths", text: "Show me a 3-month learning path" },
    { label: "How many BPO jobs are in Indore?", text: "How many BPO jobs are in Indore?" },
    { label: "मुझे क्या करना चाहिए?", text: "मुझे क्या करना चाहिए?" },
  ];

  const send = async (text) => {
    const msg = text || inp.trim();
    if (!msg || thinking) return;

    // Auto-detect Hindi
    const isHindi = [...msg].filter(c => c >= "\u0900" && c <= "\u097F").length > 2;
    if (isHindi) setLang("hi");

    setMsgs(m => [...m, { role: "user", text: msg }]);
    setInp("");
    setThinking(true);

    // Add streaming bot bubble
    const botId = "bot_" + Date.now();
    setMsgs(m => [...m, { role: "bot", text: "", id: botId, streaming: true }]);

    try {
      const res = await fetch(`${BASE_URL}/chat/message`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: sessionId,
          message: msg,
          worker_profile: workerProfile,
          language: isHindi ? "hi" : lang,
          stream: true,
        }),
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let accumulated = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        const lines = decoder.decode(value).split("\n");
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const data = JSON.parse(line.slice(6));
            if (data.token) {
              accumulated += data.token;
              setMsgs(m => m.map(x => x.id === botId ? { ...x, text: accumulated } : x));
            }
            if (data.done) break;
          } catch {}
        }
      }

      if (!accumulated) {
        setMsgs(m => m.map(x => x.id === botId
          ? { ...x, text: "Sorry, I didn't get a response. Please try again.", streaming: false }
          : x));
      } else {
        setMsgs(m => m.map(x => x.id === botId ? { ...x, streaming: false } : x));
      }

    } catch (err) {
      setMsgs(m => m.map(x => x.id === botId
        ? { ...x, text: `⚠️ Could not reach the AI backend.\n\nMake sure uvicorn is running on port 8000 and Ollama is active.\n\nError: ${err.message}`, streaming: false }
        : x));
    } finally {
      setThinking(false);
    }
  };

  const fmt = text => text.split("\n").map((ln, i) => {
    const html = ln.replace(/\*\*(.*?)\*\*/g, '<strong style="color:#93c5fd">$1</strong>');
    return <div key={i} className="mb-0.5" dangerouslySetInnerHTML={{ __html: html || "&nbsp;" }} />;
  });

  return (
    <>
      <button onClick={() => setOpen(o => !o)}
        className="fixed bottom-6 right-6 z-50 w-14 h-14 rounded-full flex items-center justify-center shadow-2xl transition-all duration-300 hover:scale-110"
        style={{ background: "linear-gradient(135deg,#3b82f6,#2563eb)", boxShadow: "0 0 20px rgba(59,130,246,0.5)" }}>
        {open
          ? <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5"><path d="M18 6L6 18M6 6l12 12" /></svg>
          : <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" /></svg>
        }
      </button>
      {open && (
        <div className="fixed bottom-24 right-6 z-50 w-96 rounded-2xl shadow-2xl flex flex-col overflow-hidden"
          style={{ background: t.chatBg, border: `1px solid ${t.border}`, height: "520px", boxShadow: "0 0 40px rgba(0,0,0,0.5)" }}>
          <div className="flex items-center justify-between px-4 py-3 border-b"
            style={{ borderColor: t.border, background: dark ? "linear-gradient(135deg,#1e2d4e,#162040)" : "#EFF6FF" }}>
            <div className="flex items-center gap-2"><Dot color={sessionReady ? "#22c55e" : "#f59e0b"} /><span className="text-sm font-bold" style={{ color: t.textPri }}>AI Career Advisor</span></div>
            <div className="flex gap-1">
              {["en", "hi"].map(l => (
                <button key={l} onClick={() => setLang(l)}
                  className="text-xs px-2 py-1 rounded transition-all"
                  style={{ background: lang === l ? "#3b82f6" : "transparent", color: lang === l ? "white" : t.textMuted }}>
                  {l === "en" ? "English" : "हिंदी"}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 overflow-y-auto px-3 py-3 space-y-3 text-xs"
            style={{ scrollbarWidth: "thin", scrollbarColor: `${t.scrollBar} transparent` }}>
            {msgs.map((m, i) => (
              <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                <div className="max-w-xs px-3 py-2 rounded-xl leading-relaxed"
                  style={{ background: m.role === "user" ? "linear-gradient(135deg,#3b82f6,#2563eb)" : t.chatBot, color: m.role === "user" ? "white" : t.chatBotTxt }}>
                  {m.role === "bot"
                    ? (m.streaming && !m.text
                        ? <div className="flex gap-1 py-1 items-center">{[0,1,2].map(j => <span key={j} className="w-1.5 h-1.5 rounded-full bg-blue-400 animate-bounce" style={{ animationDelay: `${j*0.15}s` }} />)}</div>
                        : <>{fmt(m.text)}{m.streaming && <span className="inline-block w-0.5 h-3 bg-blue-400 ml-0.5 animate-pulse align-middle" />}</>)
                    : m.text}
                </div>
              </div>
            ))}
            {thinking && !msgs.some(m => m.streaming) && (
              <div className="flex justify-start">
                <div className="px-3 py-2 rounded-xl text-xs italic animate-pulse"
                  style={{ background: t.chatBot, color: t.textMuted }}>Analyzing labor market data...</div>
              </div>
            )}
            <div ref={endRef} />
          </div>
          {msgs.length <= 1 && (
            <div className="px-3 pb-2 flex flex-wrap gap-1">
              {QUICK.map(p => (
                <button key={p.text} onClick={() => send(p.text)}
                  disabled={thinking}
                  className="text-xs px-2 py-1 rounded-full border transition-all hover:bg-blue-500/25 disabled:opacity-40"
                  style={{ background: "rgba(59,130,246,0.1)", borderColor: "rgba(59,130,246,0.3)", color: "#60a5fa" }}>
                  {p.label}
                </button>
              ))}
            </div>
          )}
          <div className="flex gap-2 px-3 py-3 border-t" style={{ borderColor: t.border }}>
            <input value={inp} onChange={e => setInp(e.target.value)} onKeyDown={e => e.key === "Enter" && !thinking && send()}
              placeholder={lang === "hi" ? "अपना सवाल लिखें..." : "Ask a question..."}
              className="flex-1 rounded-xl px-3 py-2 text-xs outline-none transition-all"
              style={{ background: t.bgInput, border: `1.5px solid ${focused ? "#3b82f6" : t.bgInputBdr}`, color: t.textPri }}
              onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
            />
            <button onClick={() => send()} disabled={!inp.trim() || thinking}
              className="w-8 h-8 rounded-xl bg-blue-500 flex items-center justify-center hover:bg-blue-400 transition-all flex-shrink-0 disabled:opacity-40 disabled:cursor-not-allowed">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5"><path d="M22 2L11 13M22 2L15 22l-4-9-9-4 20-7z" /></svg>
            </button>
          </div>
        </div>
      )}
    </>
  );
}

// ─── Pages ────────────────────────────────────────────────────────────────────

function Landing({ go }) {
  const t = useT();
  const { dark } = useTheme();

  // Live stats fetched from backend
  const [rawStats, setRawStats] = useState({ total_jobs: 0, total_cities: 0, total_skills: 0, total_ai_signals: 0 });
  useEffect(() => {
    apiGetStats()
      .then(data => setRawStats(data))
      .catch(() => {}); // silently fall back to 0 values on error
  }, []);

  const jobs  = useCounter(rawStats.total_jobs);
  const cities = useCounter(rawStats.total_cities);
  const skills = useCounter(rawStats.total_skills);
  const sigs  = useCounter(rawStats.total_ai_signals);

  const stats = [
    { label: "Live Job Listings Analyzed", value: jobs.toLocaleString(), icon: "📊", color: "#3b82f6" },
    { label: "Cities Covered", value: cities, icon: "🏙️", color: "#8b5cf6" },
    { label: "Skills Detected", value: skills.toLocaleString(), icon: "🧠", color: "#f97316" },
    { label: "AI Risk Signals Updating", value: sigs.toLocaleString(), icon: "⚡", color: "#22c55e" },
  ];
  const navCards = [
    { title: "Hiring Trends", icon: "📈", desc: "Real-time demand signals across 22 cities and 10 sectors. Track growth, decline, and emerging opportunities.", color: "#3b82f6", page: "hiring", badge: "Live Data" },
    { title: "Skills Intelligence", icon: "🧬", desc: "Rising and declining skills matrix. Gap analysis heatmap. Discover which competencies are invaluable.", color: "#8b5cf6", page: "skills", badge: "AI-Powered" },
    { title: "AI Vulnerability Index", icon: "🛡️", desc: "Automation risk scoring for every city and role. Understand which jobs are being disrupted by AI right now.", color: "#f97316", page: "risk", badge: "Critical" },
  ];
  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <div className="inline-flex items-center gap-2 rounded-full px-4 py-1.5 text-xs mb-5"
          style={{ background: "rgba(59,130,246,0.1)", border: "1px solid rgba(59,130,246,0.2)", color: "#60a5fa" }}>
          <Dot /><span>India's First AI-Powered Workforce Intelligence System</span>
        </div>
        <h1 className="text-5xl font-black mb-3 tracking-tight"
          style={{ color: t.textPri, textShadow: dark ? "0 0 40px rgba(59,130,246,0.3)" : "none" }}>
          CareerLens
        </h1>
        <p className="text-xl max-w-xl mx-auto leading-relaxed" style={{ color: t.textMuted }}>
          AI will displace millions of jobs by 2030, <span className="text-orange-400 font-semibold">will Indian workers be ready?</span>
        </p>
      </div>

      <div className="grid grid-cols-4 gap-4">
        {stats.map(s => (
          <Card key={s.label} className="p-5">
            <div className="flex items-start justify-between mb-3"><span className="text-2xl">{s.icon}</span><Dot color={s.color} /></div>
            <div className="text-3xl font-black mb-1" style={{ color: s.color, fontVariantNumeric: "tabular-nums" }}>{s.value}</div>
            <div className="text-xs" style={{ color: t.textMuted }}>{s.label}</div>
          </Card>
        ))}
      </div>

      {/* Live news ticker */}
      <Ticker />

      <div className="grid grid-cols-3 gap-5">
        {navCards.map(c => (
          <Card key={c.title} className="p-6 group" onClick={() => go(c.page)}>
            <div className="flex items-center justify-between mb-4">
              <span className="text-3xl">{c.icon}</span>
              <Pill color={c.page === "risk" ? "orange" : "blue"}>{c.badge}</Pill>
            </div>
            <h3 className="text-xl font-bold mb-2 transition-colors group-hover:text-blue-400" style={{ color: t.textPri }}>{c.title}</h3>
            <p className="text-sm leading-relaxed mb-4" style={{ color: t.textMuted }}>{c.desc}</p>
            <div className="flex items-center gap-2 text-xs font-semibold transition-all group-hover:gap-3" style={{ color: c.color }}>Explore <span>→</span></div>
          </Card>
        ))}
      </div>

      <Card className="p-8 text-center" hover={false}>
        <div className="text-4xl mb-3">🎯</div>
        <h2 className="text-2xl font-bold mb-2" style={{ color: t.textPri }}>Worker Intelligence Engine</h2>
        <p className="mb-5 max-w-md mx-auto" style={{ color: t.textMuted }}>Get your personalized AI Risk Score, Career GPS, and reskilling roadmap in under 2 minutes.</p>
        <button onClick={() => go("worker")}
          className="px-8 py-3 rounded-xl font-bold text-white transition-all hover:scale-105"
          style={{ background: "linear-gradient(135deg,#3b82f6,#2563eb)", boxShadow: "0 0 20px rgba(59,130,246,0.3)" }}>
          Analyze My Career →
        </button>
      </Card>

      {/* Feature 4: Displacement Early Warning Watchlist */}
      <Card className="p-5" hover={false}>
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <span className="text-lg">🚨</span>
            <div>
              <h3 className="font-bold" style={{ color: t.textPri }}>Displacement Early Warning</h3>
              <p className="text-xs" style={{ color: t.textMuted }}>Job categories with fastest-declining hiring — workers should start reskilling now</p>
            </div>
          </div>
          <Pill color="red">Live Watchlist</Pill>
        </div>
        <div className="grid grid-cols-3 gap-3">
          {EARLY_WARNING.map((item, i) => {
            const sc = item.severity === "critical" ? "#ef4444" : item.severity === "high" ? "#f97316" : "#f59e0b";
            const sb = item.severity === "critical" ? "rgba(239,68,68,0.07)" : item.severity === "high" ? "rgba(249,115,22,0.07)" : "rgba(234,179,8,0.07)";
            const sbd = item.severity === "critical" ? "rgba(239,68,68,0.22)" : item.severity === "high" ? "rgba(249,115,22,0.22)" : "rgba(234,179,8,0.2)";
            return (
              <div key={i} className="p-3 rounded-xl" style={{ background: sb, border: `1px solid ${sbd}` }}>
                <div className="flex items-start justify-between mb-2">
                  <span className="text-xs font-semibold leading-tight" style={{ color: t.textSec }}>{item.category}</span>
                  <span className="text-xs font-bold uppercase ml-1 flex-shrink-0" style={{ color: sc }}>{item.severity}</span>
                </div>
                <div className="text-2xl font-black" style={{ color: sc }}>-{item.declineRate}%</div>
                <div className="text-xs mt-0.5" style={{ color: t.textMuted }}>hiring decline</div>
                <div className="text-xs mt-1" style={{ color: t.textMuted }}>{item.affectedCities} cities affected</div>
                <div className="w-full rounded-full h-1 mt-2" style={{ background: "rgba(128,128,128,0.12)" }}>
                  <div className="h-full rounded-full" style={{ width: `${Math.min(item.declineRate, 100)}%`, background: sc }} />
                </div>
                <p className="text-xs mt-2 leading-relaxed" style={{ color: t.textMuted, opacity: 0.85 }}>{item.msg}</p>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}

function HiringPage() {
  const t = useT();
  const [city, setCity] = useState("Bangalore");
  const [range, setRange] = useState("30 days");
  const [sector, setSector] = useState("All Sectors");
  const sectorMap = HIRING_BY_SECTOR[sector] || HIRING_BY_SECTOR["All Sectors"];
  const data = sectorMap[city] || sectorMap.default || HIRING_DATA[city] || HIRING_DATA.default;
  const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const insights = [
    "AI Engineer hiring in Ahmedabad grew 82% in the last 30 days",
    "Data Scientist roles in Bangalore up 61% vs last quarter",
    "BPO job postings in Lucknow declined 38% — AI automation signal",
    "Cloud Architect demand in Hyderabad surged 55% this month",
    "Fintech roles in Mumbai showing 44% YoY growth",
  ];
  const topRoles = [
    { role: "AI/ML Engineer", growth: "+84%", city: "Bangalore" },
    { role: "Cloud Architect", growth: "+65%", city: "Hyderabad" },
    { role: "Data Engineer", growth: "+58%", city: "Pune" },
    { role: "DevOps Engineer", growth: "+51%", city: "Chennai" },
    { role: "Product Analyst", growth: "+47%", city: "Mumbai" },
    { role: "Cybersecurity Analyst", growth: "+43%", city: "Delhi" },
  ];
  const selSt = { background: t.bgInput, border: `1.5px solid ${t.bgInputBdr}`, color: t.textPri };
  return (
    <div className="grid grid-cols-4 gap-5">
      <div className="col-span-3 space-y-5">
        <Card className="p-4">
          <div className="flex items-center gap-4 flex-wrap">
            <div className="flex items-center gap-2">
              <span className="text-xs" style={{ color: t.textMuted }}>City</span>
              <select value={city} onChange={e => setCity(e.target.value)} className="rounded-lg px-3 py-1.5 text-sm outline-none cursor-pointer" style={selSt}>
                {CITIES.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs" style={{ color: t.textMuted }}>Sector</span>
              <select value={sector} onChange={e => setSector(e.target.value)} className="rounded-lg px-3 py-1.5 text-sm outline-none cursor-pointer" style={selSt}>
                {["All Sectors", ...SECTORS].map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
            <div className="flex gap-1">
              {["7 days","30 days","90 days","1 year"].map(r => (
                <button key={r} onClick={() => setRange(r)} className="text-xs px-3 py-1.5 rounded-lg transition-all"
                  style={{ background: range === r ? "#3b82f6" : t.bgInput, color: range === r ? "white" : t.textMuted, border: `1px solid ${range === r ? "#3b82f6" : t.bgInputBdr}` }}>
                  {r}
                </button>
              ))}
            </div>
          </div>
        </Card>
        <Card className="p-5" hover={false}>
          <div className="flex items-center justify-between mb-4">
            <div><h3 className="font-bold" style={{ color: t.textPri }}>City Hiring Trend</h3><p className="text-xs" style={{ color: t.textMuted }}>{city} • {sector} • {range}</p></div>
            <Pill color="green">Live</Pill>
          </div>
          <LineSVG data={data} color="#3b82f6" w={600} h={160} />
          <div className="flex justify-between mt-2">{MONTHS.map(m => <span key={m} className="text-xs" style={{ color: t.textMuted }}>{m}</span>)}</div>
        </Card>
        <Card className="p-5" hover={false}>
          <h3 className="font-bold mb-4" style={{ color: t.textPri }}>Sector Job Growth</h3>
          <div className="space-y-2">
            {Object.entries(SECTORS_DATA).map(([s, vals]) => {
              const g = ((vals[11] - vals[0]) / vals[0] * 100).toFixed(0);
              const pos = g > 0;
              return (
                <div key={s} className="flex items-center gap-3">
                  <div className="w-40 text-xs truncate" style={{ color: t.textSec }}>{s}</div>
                  <div className="flex-1 flex items-center gap-2">
                    <div className="flex-1 rounded-full h-2 overflow-hidden" style={{ background: "rgba(128,128,128,0.15)" }}>
                      <div className="h-full rounded-full" style={{ width: `${Math.min(Math.abs(g), 100)}%`, background: pos ? "#3b82f6" : "#ef4444" }} />
                    </div>
                    <span className="text-xs font-bold w-14 text-right" style={{ color: pos ? "#22c55e" : "#ef4444" }}>{pos ? "+" : ""}{g}%</span>
                  </div>
                  <div className="w-16"><MiniBar data={vals.slice(-6)} color={pos ? "#3b82f6" : "#ef4444"} height={24} /></div>
                </div>
              );
            })}
          </div>
        </Card>
      </div>
      <div className="space-y-4">
        <Card className="p-4" hover={false}>
          <div className="flex items-center gap-2 mb-3"><Dot color="#22c55e" /><span className="text-sm font-bold" style={{ color: t.textPri }}>Live Insights</span></div>
          <div className="space-y-3">
            {insights.map((ins, i) => (
              <div key={i} className="p-3 rounded-xl" style={{ background: "rgba(59,130,246,0.07)", border: "1px solid rgba(59,130,246,0.15)" }}>
                <p className="text-xs leading-relaxed" style={{ color: t.textSec }}>{ins}</p>
                <span className="text-xs text-blue-400 mt-1 block">{i + 1}m ago</span>
              </div>
            ))}
          </div>
        </Card>
        <Card className="p-4" hover={false}>
          <h3 className="text-sm font-bold mb-3" style={{ color: t.textPri }}>Top Growing Roles</h3>
          <div className="space-y-2">
            {topRoles.map((r, i) => (
              <div key={i} className="flex items-center justify-between py-1.5 border-b last:border-0" style={{ borderColor: t.border }}>
                <div><div className="text-xs font-medium" style={{ color: t.textPri }}>{r.role}</div><div className="text-xs" style={{ color: t.textMuted }}>{r.city}</div></div>
                <Pill color="green">{r.growth}</Pill>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  );
}

function SkillsPage() {
  const t = useT();
  const hmap = RISING.slice(0, 8).map(s => ({ skill: s.skill, demand: s.demand, training: Math.floor(s.demand * 0.45 + 10), gap: Math.floor(s.demand * 0.55 - 5) }));
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-5">
        <Card className="p-5" hover={false}>
          <div className="flex items-center justify-between mb-4"><h3 className="font-bold" style={{ color: t.textPri }}>Rising Skills</h3><Pill color="green">Top 20</Pill></div>
          <div className="space-y-1.5 max-h-96 overflow-y-auto pr-1" style={{ scrollbarWidth: "thin", scrollbarColor: `${t.scrollBar} transparent` }}>
            {RISING.map((s, i) => (
              <div key={i} className="flex items-center gap-3 py-1.5 border-b last:border-0" style={{ borderColor: t.border }}>
                <span className="text-xs w-4" style={{ color: t.textMuted }}>{i + 1}</span>
                <div className="flex-1">
                  <div className="text-xs font-medium" style={{ color: t.textSec }}>{s.skill}</div>
                  <div className="w-full rounded-full h-1 mt-1" style={{ background: "rgba(128,128,128,0.15)" }}>
                    <div className="bg-blue-500 h-1 rounded-full" style={{ width: `${s.demand}%` }} />
                  </div>
                </div>
                <span className="text-xs font-bold w-12 text-right text-green-500">+{s.change}%</span>
              </div>
            ))}
          </div>
        </Card>
        <Card className="p-5" hover={false}>
          <div className="flex items-center justify-between mb-4"><h3 className="font-bold" style={{ color: t.textPri }}>Declining Skills</h3><Pill color="red">Automation Risk</Pill></div>
          <div className="space-y-1.5 max-h-96 overflow-y-auto pr-1" style={{ scrollbarWidth: "thin", scrollbarColor: `${t.scrollBar} transparent` }}>
            {DECLINING.map((s, i) => (
              <div key={i} className="flex items-center gap-3 py-1.5 border-b last:border-0" style={{ borderColor: t.border }}>
                <span className="text-xs w-4" style={{ color: t.textMuted }}>{i + 1}</span>
                <div className="flex-1">
                  <div className="text-xs font-medium" style={{ color: t.textSec }}>{s.skill}</div>
                  <div className="w-full rounded-full h-1 mt-1" style={{ background: "rgba(128,128,128,0.15)" }}>
                    <div className="bg-red-500 h-1 rounded-full" style={{ width: `${s.demand}%` }} />
                  </div>
                </div>
                <span className="text-xs font-bold w-12 text-right text-red-400">-{s.change}%</span>
              </div>
            ))}
          </div>
        </Card>
      </div>
      <Card className="p-5" hover={false}>
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-bold" style={{ color: t.textPri }}>Skill Gap Matrix Heatmap</h3>
          <div className="flex gap-3 text-xs" style={{ color: t.textMuted }}>
            {[["Low Gap", "rgba(34,197,94,0.5)"], ["Medium", "rgba(234,179,8,0.5)"], ["High Gap", "rgba(239,68,68,0.6)"]].map(([l, c]) => (
              <span key={l} className="flex items-center gap-1"><span className="w-3 h-3 rounded inline-block" style={{ background: c }} />{l}</span>
            ))}
          </div>
        </div>
        <table className="w-full">
          <thead><tr>{["Skill", "Market Demand", "Training Availability", "Gap Score"].map(h => <th key={h} className="text-xs text-left pb-2 pr-4" style={{ color: t.textMuted }}>{h}</th>)}</tr></thead>
          <tbody>{hmap.map((row, i) => (
            <tr key={i} className="border-t" style={{ borderColor: t.border }}>
              <td className="py-1.5 pr-4 text-xs whitespace-nowrap" style={{ color: t.textSec }}>{row.skill}</td>
              <td className="py-1.5 px-2"><HeatCell value={row.demand} /></td>
              <td className="py-1.5 px-2"><HeatCell value={row.training} /></td>
              <td className="py-1.5 px-2"><HeatCell value={row.gap} /></td>
            </tr>
          ))}</tbody>
        </table>
      </Card>
      <Card className="p-5" hover={false}>
        <h3 className="font-bold mb-1" style={{ color: t.textPri }}>Skill Trend — 12 Month Popularity</h3>
        <p className="text-xs mb-4" style={{ color: t.textMuted }}>Top 5 trending skills · Hover any point to see month & value</p>
        <div className="space-y-3">
          {RISING.slice(0, 5).map((s, i) => {
            const trend = Array.from({ length: 12 }, (_, j) => Math.max(10, s.demand - (12 - j) * (s.change / 120)));
            const colors = ["#3b82f6", "#8b5cf6", "#f97316", "#22c55e", "#ec4899"];
            return (
              <div key={i} className="flex items-center gap-3">
                <span className="text-xs w-36 truncate" style={{ color: t.textMuted }}>{s.skill}</span>
                <div className="flex-1"><LineSVG data={trend} color={colors[i]} w={400} h={40} showLabels={true} skillName={s.skill} /></div>
                <span className="text-xs font-bold text-green-500">+{s.change}%</span>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}

function RiskPage() {
  const t = useT();
  const [sel, setSel] = useState("Delhi");
  const [empSector, setEmpSector] = useState("IT & Software");
  const [viewMode, setViewMode] = useState("worker"); // "worker" | "employer"

  // ── API 1: GET /api/city-risk ──────────────────────────────────────────────
const [cityRiskData, setCityRiskData] = useState({});
const [cityRiskLoading, setCityRiskLoading] = useState(true);
const [cityRiskError, setCityRiskError] = useState("");

useEffect(() => {
  async function loadCityRisk() {
    try {
      setCityRiskLoading(true);
      const data = await apiGetCityRisk();
      setCityRiskData(data);
      setCityRiskError("");
    } catch (err) {
      setCityRiskError(err.message);
    } finally {
      setCityRiskLoading(false);
    }
  }
  loadCityRisk();
}, []);

  // ── API 2: GET /api/high-risk-jobs ────────────────────────────────────────
  const [highRiskJobs, setHighRiskJobs] = useState([]);
const [highRiskJobsLoading, setHighRiskJobsLoading] = useState(true);
const [highRiskJobsError, setHighRiskJobsError] = useState("");
useEffect(() => {
  async function loadHighRiskJobs() {
    try {
      setHighRiskJobsLoading(true);
      const data = await apiGetHighRiskJobs();
      setHighRiskJobs(data);
      setHighRiskJobsError("");
    } catch (err) {
      setHighRiskJobsError(err.message);
    } finally {
      setHighRiskJobsLoading(false);
    }
  }
  loadHighRiskJobs();
}, []);
  const [highRiskLoading, setHighRiskLoading] = useState(false);
  const [highRiskError, setHighRiskError] = useState(null);

  useEffect(() => {
    setHighRiskLoading(true);
    apiGetHighRiskJobs()
      .then(data => {
        // API returns [ { role, city, risk }, ... ]
        if (Array.isArray(data) && data.length > 0) setHighRiskJobs(data);
        setHighRiskError(null);
      })
      .catch(err => {
        console.warn("high-risk-jobs API unavailable, using fallback:", err.message);
        setHighRiskError(err.message);
      })
      .finally(() => setHighRiskLoading(false));
  }, []);

  const cityRoles = {
    Delhi: { risky: ["Call Center Agent", "Data Entry Clerk", "Cashier"], safe: ["AI Policy Analyst", "Cybersecurity Specialist", "Urban Planner"] },
    Mumbai: { risky: ["Bank Teller", "Stock Trader (Manual)", "Basic Accountant"], safe: ["Fintech Developer", "AI Compliance Officer", "UX Designer"] },
    Bangalore: { risky: ["Manual QA Tester", "Legacy Coder", "Basic IT Support"], safe: ["AI/ML Engineer", "DevOps Lead", "Product Manager"] },
    default: { risky: ["Data Entry", "Manual Caller", "Basic Support"], safe: ["AI Specialist", "Data Analyst", "Digital Marketer"] },
  };
  const sorted = [...CITIES].sort((a, b) => (cityRiskData[b] || 0) - (cityRiskData[a] || 0));
  const info = cityRoles[sel] || cityRoles.default;
  const jobRanking = CITY_JOB_RANKINGS[sel] || CITY_JOB_RANKINGS.default;
  const empData = EMPLOYER_SUPPLY_DEMAND[empSector] || [];
  const selSt = { background: t.bgInput, border: `1.5px solid ${t.bgInputBdr}`, color: t.textPri };
  return (
    <div className="space-y-4">
      {/* Worker / Employer toggle */}
      <div className="flex items-center gap-2">
        {[["worker","👤 Worker View"],["employer","🏢 Employer View"]].map(([m,lbl]) => (
          <button key={m} onClick={() => setViewMode(m)}
            className="text-xs px-4 py-1.5 rounded-lg font-semibold transition-all"
            style={{ background: viewMode === m ? "#3b82f6" : t.bgInput, color: viewMode === m ? "white" : t.textMuted, border: `1px solid ${viewMode === m ? "#3b82f6" : t.bgInputBdr}` }}>
            {lbl}
          </button>
        ))}
      </div>

      {/* ── Employer view (Feature 5) ── */}
      {viewMode === "employer" && (
        <Card className="p-5" hover={false}>
          <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
            <div>
              <h3 className="font-bold" style={{ color: t.textPri }}>Skill Supply vs Demand Gap</h3>
              <p className="text-xs mt-0.5" style={{ color: t.textMuted }}>What employers need vs what candidates offer — by sector</p>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs" style={{ color: t.textMuted }}>Sector</span>
              <select value={empSector} onChange={e => setEmpSector(e.target.value)} className="rounded-lg px-3 py-1.5 text-sm outline-none cursor-pointer" style={selSt}>
                {Object.keys(EMPLOYER_SUPPLY_DEMAND).map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
          </div>
          <div className="flex gap-4 mb-4 text-xs" style={{ color: t.textMuted }}>
            {[["Supply (Available)","#22c55e"],["Demand (Needed)","#3b82f6"],["Gap","#ef4444"]].map(([l,c]) => (
              <span key={l} className="flex items-center gap-1.5"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: c }} />{l}</span>
            ))}
          </div>
          <div className="space-y-4">
            {empData.map((row, i) => (
              <div key={i}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-medium" style={{ color: t.textSec }}>{row.skill}</span>
                  <span className="text-xs font-bold text-red-400">Gap: {row.demand - row.supply}</span>
                </div>
                <div className="space-y-0.5">
                  {[{label:"Supply",val:row.supply,color:"#22c55e"},{label:"Demand",val:row.demand,color:"#3b82f6"},{label:"Gap",val:row.demand-row.supply,color:"#ef4444"}].map(bar => (
                    <div key={bar.label} className="flex items-center gap-2">
                      <div className="w-12 text-xs text-right" style={{ color: t.textMuted }}>{bar.label}</div>
                      <div className="flex-1 rounded-full h-1.5 overflow-hidden" style={{ background: "rgba(128,128,128,0.15)" }}>
                        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${bar.val}%`, background: bar.color }} />
                      </div>
                      <span className="text-xs w-6 text-right font-semibold" style={{ color: bar.color }}>{bar.val}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* ── Worker view (original layout, unchanged) ── */}
      {viewMode === "worker" && (
    <div className="grid grid-cols-3 gap-5">
      <div className="col-span-2 space-y-5">
        <Card className="p-5" hover={false}>
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-bold" style={{ color: t.textPri }}>India AI Risk Heatmap</h3>
            <div className="flex gap-3 text-xs" style={{ color: t.textMuted }}>
              {[["Low", "#22c55e"], ["Medium", "#f59e0b"], ["High", "#ef4444"]].map(([l, c]) => (
                <span key={l} className="flex items-center gap-1"><span className="w-3 h-3 rounded-full inline-block" style={{ background: c }} />{l}</span>
              ))}
            </div>
          </div>
          <div className="grid grid-cols-4 gap-2">
            {sorted.map(city => {
              const risk = cityRiskData[city] || 0, color = riskColor(risk), isSel = sel === city;
              return (
                <div key={city} onClick={() => setSel(city)}
                  className="rounded-xl p-3 cursor-pointer transition-all duration-200 hover:scale-105"
                  style={{ background: `${color}18`, border: `1.5px solid ${isSel ? color : `${color}40`}`, boxShadow: isSel ? `0 0 14px ${color}50` : "none" }}>
                  <div className="text-xs font-medium mb-1" style={{ color: t.textSec }}>{city}</div>
                  <div className="text-xl font-black" style={{ color }}>{risk}</div>
                  <div className="text-xs font-semibold" style={{ color }}>{riskLabel(risk)}</div>
                </div>
              );
            })}
          </div>
        </Card>
        <Card className="p-5" hover={false}>
          <h3 className="font-bold mb-4" style={{ color: t.textPri }}>Highest Risk Job Roles</h3>
          {highRiskLoading && <div className="text-xs text-center py-4 animate-pulse" style={{ color: t.textMuted }}>Loading job data...</div>}
          {highRiskError && <div className="text-xs text-orange-400 mb-2 px-1">⚠ Using cached data — API unavailable</div>}
          <table className="w-full">
            <thead><tr className="border-b" style={{ borderColor: t.border }}>{["#", "Role", "City", "AI Risk Score"].map(h => <th key={h} className="text-xs text-left pb-2 pr-4" style={{ color: t.textMuted }}>{h}</th>)}</tr></thead>
            <tbody>
              {highRiskJobs.map((r, i) => (
                <tr key={i} className="border-b last:border-0" style={{ borderColor: t.border }}>
                  <td className="py-2 text-xs" style={{ color: t.textMuted }}>{i + 1}</td>
                  <td className="py-2 text-xs font-medium" style={{ color: t.textSec }}>{r.role}</td>
                  <td className="py-2 text-xs" style={{ color: t.textMuted }}>{r.city}</td>
                  <td className="py-2">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 max-w-20 rounded-full h-1.5" style={{ background: "rgba(128,128,128,0.15)" }}>
                        <div className="h-full rounded-full" style={{ width: `${r.risk}%`, background: riskColor(r.risk) }} />
                      </div>
                      <span className="text-xs font-bold" style={{ color: riskColor(r.risk) }}>{r.risk}</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </div>
      <div className="space-y-4">
        <Card className="p-4" hover={false}>
          <h3 className="text-sm font-bold mb-1" style={{ color: t.textPri }}>Risk Explanation</h3>
          <p className="text-xs mb-3" style={{ color: t.textMuted }}>Click any city on the map</p>
          <div className="text-center mb-4">
            <div className="text-3xl font-black mb-1" style={{ color: riskColor(cityRiskData[sel] || 0) }}>{sel}</div>
            <div className="text-5xl font-black" style={{ color: riskColor(cityRiskData[sel] || 0) }}>{cityRiskData[sel] || "—"}</div>
            <Pill color={(cityRiskData[sel] || 0) > 66 ? "red" : (cityRiskData[sel] || 0) > 33 ? "yellow" : "green"}>{riskLabel(cityRiskData[sel] || 0)}</Pill>
          </div>
          {[
            { label: "Hiring Decline", val: "-38%", sub: "vs previous quarter", c: "#ef4444", bg: "rgba(239,68,68,0.08)", bdr: "rgba(239,68,68,0.2)" },
            { label: "AI Tool Mentions", val: "67%", sub: "of job postings", c: "#f97316", bg: "rgba(249,115,22,0.08)", bdr: "rgba(249,115,22,0.2)" },
            { label: "Automation Probability", val: `${cityRiskData[sel] || 0}%`, sub: "within 5 years", c: "#f59e0b", bg: "rgba(234,179,8,0.08)", bdr: "rgba(234,179,8,0.2)" },
          ].map(r => (
            <div key={r.label} className="p-3 rounded-xl mb-2" style={{ background: r.bg, border: `1px solid ${r.bdr}` }}>
              <div className="text-xs font-bold mb-0.5" style={{ color: r.c }}>{r.label}</div>
              <div className="text-sm font-black" style={{ color: r.c }}>{r.val}</div>
              <div className="text-xs" style={{ color: t.textMuted }}>{r.sub}</div>
            </div>
          ))}
        </Card>
        <Card className="p-4" hover={false}>
          <h3 className="text-sm font-bold text-red-400 mb-3">⚠️ Risky Jobs in {sel}</h3>
          <div className="space-y-1.5">{info.risky.map(r => <div key={r} className="text-xs py-1.5 px-3 rounded-lg text-red-300" style={{ background: "rgba(239,68,68,0.08)", border: "1px solid rgba(239,68,68,0.2)" }}>{r}</div>)}</div>
          <h3 className="text-sm font-bold text-green-500 mb-3 mt-4">✅ Safe Jobs in {sel}</h3>
          <div className="space-y-1.5">{info.safe.map(r => <div key={r} className="text-xs py-1.5 px-3 rounded-lg text-green-300" style={{ background: "rgba(34,197,94,0.08)", border: "1px solid rgba(34,197,94,0.2)" }}>{r}</div>)}</div>
        </Card>
        {/* Feature 3: City job risk rankings */}
        <Card className="p-4" hover={false}>
          <h3 className="text-sm font-bold mb-3" style={{ color: t.textPri }}>Job Risk Ranking — {sel}</h3>
          <div className="mb-3">
            <div className="text-xs font-semibold text-red-400 mb-2">🔴 5 Highest-Risk Jobs</div>
            <div className="space-y-1.5">
              {jobRanking.highest.map((job, i) => (
                <div key={i} className="flex items-center justify-between py-1 px-2 rounded-lg"
                  style={{ background: "rgba(239,68,68,0.06)", border: "1px solid rgba(239,68,68,0.15)" }}>
                  <div className="flex items-center gap-1.5">
                    <span className="text-xs" style={{ color: t.textMuted }}>{i + 1}.</span>
                    <span className="text-xs" style={{ color: t.textSec }}>{job.role}</span>
                  </div>
                  <span className="text-xs font-bold" style={{ color: riskColor(job.risk) }}>{job.risk}</span>
                </div>
              ))}
            </div>
          </div>
          <div>
            <div className="text-xs font-semibold text-green-500 mb-2">🟢 5 Lowest-Risk Jobs</div>
            <div className="space-y-1.5">
              {jobRanking.lowest.map((job, i) => (
                <div key={i} className="flex items-center justify-between py-1 px-2 rounded-lg"
                  style={{ background: "rgba(34,197,94,0.06)", border: "1px solid rgba(34,197,94,0.15)" }}>
                  <div className="flex items-center gap-1.5">
                    <span className="text-xs" style={{ color: t.textMuted }}>{i + 1}.</span>
                    <span className="text-xs" style={{ color: t.textSec }}>{job.role}</span>
                  </div>
                  <span className="text-xs font-bold text-green-500">{job.risk}</span>
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>
    </div>
      )}
    </div>
  );
}

function WorkerPage({ onProfileChange }) {
  const t = useT();
  const [step, setStep] = useState("form");
  const [form, setForm] = useState({ title: "", city: "Bangalore", experience: "", tasks: "" });
  const [apiError, setApiError] = useState(null);

  // Notify Shell whenever form fields change so Chatbot always has latest data
  useEffect(() => {
    if (form.title) onProfileChange?.({ job_title: form.title, city: form.city, years_experience: Number(form.experience) || 0, write_up: form.tasks, risk_score: 74 });
  }, [form]);

  // ── API 3 response state (POST /api/analyze-worker) ───────────────────────
  // Fallback values shown until real API responds
  const [riskScore, setRiskScore]       = useState(74);
  const [wSkills, setWSkills]           = useState([72, 65, 80, 28, 35]);
  const [mSkills, setMSkills]           = useState([45, 88, 60, 91, 78]);
  const [skillLabels, setSkillLabels]   = useState(["Communication", "CRM Systems", "Support", "AI Tools", "Data Analysis"]);
  const [missing, setMissing]           = useState(["AI Support Tools", "Customer Success Metrics", "Automation Platforms", "Data Visualization"]);
  const [careerNodes, setCareerNodes]   = useState([
    { role: "BPO Executive", risk: 87, salary: "Current", skills: ["Call Handling", "Scripts"] },
    { role: "Customer Support Specialist", risk: 62, salary: "+18% salary", skills: ["CRM", "Ticketing", "Analytics"] },
    { role: "Customer Success Manager", risk: 38, salary: "+35% salary", skills: ["Strategy", "Retention", "AI Tools"] },
    { role: "AI CX Analyst", risk: 14, salary: "+58% salary", skills: ["ML", "NLP", "Dashboards"] },
  ]);

  // ── API 4 response state (POST /predict) ─────────────────────────────────
  // predict returns { risk_score: 0.72, message } — risk_score is 0-1 float
  // We convert to 0-100 int and merge with analyze-worker score
  const [predictScore, setPredictScore] = useState(null); // null = not yet fetched

  // ── API 5 response state (POST /api/course-recommendations) ──────────────
  const [courses, setCourses] = useState(COURSES); // fallback to constant
  const [coursesLoading, setCoursesLoading] = useState(false);

  // ── API 6 response state (GET /api/job-impact?ai_level) ──────────────────
  const [aiAdoption, setAiAdoption]     = useState(50);
  const [jobImpact, setJobImpact]       = useState(null); // null = use computed fallback
  const [jobImpactLoading, setJobImpactLoading] = useState(false);

  // Derived fallback values used when API 6 hasn't responded yet
  const fallbackBpoAt = Math.max(0, 100 - aiAdoption * 0.7).toFixed(0);
  const fallbackAiAt  = Math.min(200, 20 + aiAdoption * 1.8).toFixed(0);
  const bpoAt  = jobImpact ? jobImpact.bpoRemaining : fallbackBpoAt;
  const aiAt   = jobImpact ? jobImpact.aiGrowth     : fallbackAiAt;
  // userRisk from API 6 overrides the computed fallback if present
  const displayRisk = jobImpact
    ? jobImpact.userRisk
    : Math.max(0, riskScore - aiAdoption * 0.3).toFixed(0);

  // Fetch job-impact whenever aiAdoption slider changes (debounced 400ms)
  useEffect(() => {
    const timer = setTimeout(() => {
      setJobImpactLoading(true);
      apiGetJobImpact(aiAdoption)
        .then(data => setJobImpact(data))
        .catch(err => {
          console.warn("job-impact API unavailable, using formula fallback:", err.message);
          setJobImpact(null); // revert to formula
        })
        .finally(() => setJobImpactLoading(false));
    }, 400);
    return () => clearTimeout(timer);
  }, [aiAdoption]);

  // ── Submit handler — calls APIs 3, 4, and 5 in parallel ──────────────────
  const handleSubmit = async () => {
    if (!form.title || !form.tasks) return;
    setStep("loading");
    setApiError(null);
    try {
      // Fire all three POST calls in parallel
      const [workerResult, predictResult, coursesResult] = await Promise.allSettled([
        apiAnalyzeWorker(form),
        apiPredict({
          name:       form.title,
          age:        0,          // not collected in form; backend can handle 0
          gender:     "",         // not collected in form
          department: form.city,  // closest available field
          experience: Number(form.experience) || 0,
        }),
        apiGetCourseRecommendations({ title: form.title, city: form.city, experience: form.experience }),
      ]);

      // API 3 — analyze-worker
      if (workerResult.status === "fulfilled") {
  const d = workerResult.value;

  if (typeof d.risk_score === "number") {
    setRiskScore(d.risk_score <= 1 ? Math.round(d.risk_score * 100) : Math.round(d.risk_score));
  }

  // Only update Radar data when all three arrays exist AND have matching lengths.
  // If any one is missing or lengths differ, the demo-data fallback stays intact
  // so the Skill DNA graph always renders correctly.
  const _apiWorker = Array.isArray(d.worker_skills) ? d.worker_skills : null;
  const _apiMarket = Array.isArray(d.market_skills) ? d.market_skills : null;
  const _apiLabels = Array.isArray(d.skill_labels)  ? d.skill_labels  : null;
  if (
    _apiWorker && _apiMarket && _apiLabels &&
    _apiWorker.length === _apiMarket.length &&
    _apiWorker.length === _apiLabels.length &&
    _apiWorker.length >= 3
  ) {
    setWSkills(_apiWorker);
    setMSkills(_apiMarket);
    setSkillLabels(_apiLabels);
  }

  if (Array.isArray(d.missing_skills)) {
    setMissing(d.missing_skills);
  }

  if (Array.isArray(d.career_path)) {
    // Normalise API field names to the shape CareerNode expects:
    // role (string), risk (0-100 int), salary (string), skills (string[])
    setCareerNodes(d.career_path.map(node => ({
      role:   node.role   || node.title         || node.job_title   || "Unknown Role",
      risk:   typeof node.risk === "number"
                ? (node.risk <= 1 ? Math.round(node.risk * 100) : Math.round(node.risk))
                : typeof node.risk_score === "number"
                    ? (node.risk_score <= 1 ? Math.round(node.risk_score * 100) : Math.round(node.risk_score))
                    : 50,
      salary: node.salary || node.salary_change || node.salary_impact || "—",
      skills: Array.isArray(node.skills)          ? node.skills
            : Array.isArray(node.required_skills) ? node.required_skills
            : Array.isArray(node.key_skills)      ? node.key_skills
            : [],
    })));
  }
} else {
  console.warn("analyze-worker API failed:", workerResult.reason?.message);
}

      // API 4 — /predict
      if (predictResult.status === "fulfilled") {
        const d = predictResult.value;
        // risk_score is 0–1 float → convert to 0–100
        if (typeof d.risk_score === "number") {
          const score = d.risk_score <= 1 ? Math.round(d.risk_score * 100) : Math.round(d.risk_score);
          setPredictScore(score);
          // If analyze-worker didn't return a score, use this one
          setRiskScore(prev => prev === 74 ? score : prev);
        }
      } else {
        console.warn("/predict API failed:", predictResult.reason?.message);
      }

      // API 5 — course-recommendations
      if (coursesResult.status === "fulfilled") {
        setCoursesLoading(true);
        const data = coursesResult.value;
        // Require ≥2 courses before replacing the full COURSES fallback.
        // Spread order: ...c first, then computed fields last — so our month/free
        // values are not silently overridden by stale fields in the API object.
        if (Array.isArray(data) && data.length >= 2) {
          setCourses(data.map((c, i) => ({ ...c, month: c.month ?? i + 1, free: c.free ?? false })));
        }
        setCoursesLoading(false);
      } else {
        console.warn("course-recommendations API failed:", coursesResult.reason?.message);
        // courses stays as COURSES fallback
      }

      setStep("report");
    } catch (err) {
      console.error("Unexpected error during analysis:", err);
      setApiError("Analysis failed. Showing cached results.");
      setStep("report"); // still show report with fallback data
    }
  };

  if (step === "loading") return (
    <div className="flex flex-col items-center justify-center h-96 gap-4">
      <div className="relative w-20 h-20">
        <div className="absolute inset-0 rounded-full border-4 border-blue-500/20 animate-ping" />
        <div className="absolute inset-2 rounded-full border-4 border-t-blue-500 border-r-blue-500/40 border-b-blue-500/20 border-l-blue-500/40 animate-spin" />
        <div className="absolute inset-6 rounded-full bg-blue-500/30" />
      </div>
      <div className="text-lg font-bold" style={{ color: t.textPri }}>Analyzing Labor Market Data...</div>
      <div className="text-sm animate-pulse" style={{ color: t.textMuted }}>Processing 2.8M job listings • Scoring skill DNA • Mapping career pathways</div>
    </div>
  );

  if (step === "report") return (
    <div className="space-y-5">
      {apiError && (
        <div className="text-xs text-orange-400 px-4 py-2 rounded-xl" style={{ background: "rgba(249,115,22,0.08)", border: "1px solid rgba(249,115,22,0.2)" }}>
          ⚠ {apiError}
        </div>
      )}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-black" style={{ color: t.textPri }}>Career Intelligence Report</h2>
          <p className="text-sm" style={{ color: t.textMuted }}>Generated for: {form.title} • {form.city} • {form.experience} yrs</p>
        </div>
        <button onClick={() => setStep("form")} className="text-xs border px-3 py-1.5 rounded-lg" style={{ color: t.textMuted, borderColor: t.border }}>← New Analysis</button>
      </div>
      <div className="grid grid-cols-3 gap-5">
        <Card className="p-5 text-center" hover={false}>
          <h3 className="text-sm font-bold mb-2" style={{ color: t.textSec }}>AI Risk Score</h3>
          <div className="flex justify-center"><Gauge score={riskScore} size={160} /></div>
          <div className="text-2xl font-black mt-1" style={{ color: riskColor(riskScore) }}>{riskLabel(riskScore)}</div>
          {predictScore !== null && predictScore !== riskScore && (
            <p className="text-xs mt-1" style={{ color: t.textMuted }}>Prediction model: {predictScore}/100</p>
          )}
          <p className="text-xs mt-1" style={{ color: t.textMuted }}>High automation probability within 5 years</p>
        </Card>
        <Card className="p-5" hover={false}>
          <h3 className="text-sm font-bold mb-2" style={{ color: t.textSec }}>Skill DNA</h3>
          <div className="flex justify-center"><Radar workerData={wSkills} marketData={mSkills} labels={skillLabels} size={220} /></div>
          <div className="flex gap-4 justify-center text-xs mt-1" style={{ color: t.textMuted }}>
            <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-orange-400 inline-block" />Your Skills</span>
            <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-blue-400 inline-block" />Market</span>
          </div>
        </Card>
        <Card className="p-5" hover={false}>
          <h3 className="text-sm font-bold mb-1" style={{ color: t.textSec }}>Skill Gap Analysis</h3>
          <p className="text-xs text-red-400 mb-3">You are missing:</p>
          <div className="space-y-2 mb-4">
            {missing.map(s => <div key={s} className="flex items-center gap-2 p-2 rounded-lg" style={{ background: "rgba(239,68,68,0.08)", border: "1px solid rgba(239,68,68,0.2)" }}><span className="text-red-400">⚠</span><span className="text-xs text-red-300">{s}</span></div>)}
          </div>
          <p className="text-xs text-green-500 mb-2">You excel at:</p>
          {["Communication (72/100)", "Customer Empathy (80/100)"].map(s => (
            <div key={s} className="flex items-center gap-2 p-2 rounded-lg mb-1" style={{ background: "rgba(34,197,94,0.08)", border: "1px solid rgba(34,197,94,0.2)" }}><span className="text-green-500">✓</span><span className="text-xs text-green-300">{s}</span></div>
          ))}
        </Card>
      </div>
      <Card className="p-5" hover={false}>
        <h3 className="font-bold mb-1" style={{ color: t.textPri }}>Career GPS</h3>
        <p className="text-xs mb-5" style={{ color: t.textMuted }}>Your transition pathway from high-risk to future-proof roles</p>
        <div className="flex justify-center"><div className="flex flex-col items-center">
          {careerNodes.map((n, i) => <CareerNode key={i} {...n} isFirst={i === 0} />)}
        </div></div>
      </Card>
      <Card className="p-5" hover={false} xStyle={{ background: "linear-gradient(135deg,rgba(59,130,246,0.08),rgba(37,99,235,0.04))" }}>
        <div className="flex items-start gap-3">
          <div className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0 mt-0.5" style={{ background: "rgba(59,130,246,0.2)" }}>🧠</div>
          <div>
            <h3 className="text-sm font-bold mb-2" style={{ color: t.textPri }}>Personalized Career Intelligence</h3>
            <p className="text-sm leading-relaxed" style={{ color: t.textSec }}>
              Your profile shows strong communication and problem-solving skills. The market is shifting toward AI-assisted support systems.{" "}
              <span className="text-blue-400 font-medium">Learning automation platforms and customer success analytics</span> would reduce your risk from{" "}
              <span className="text-red-400 font-bold">{riskScore}</span> to <span className="text-green-500 font-bold">{Math.max(0, riskScore - 43)}</span> within 6 months. Your customer empathy is your competitive moat.
            </p>
          </div>
        </div>
      </Card>

      {/* Learning Path — API 5: POST /api/course-recommendations */}
      <Card className="p-5" hover={false}>
        <h3 className="font-bold mb-4" style={{ color: t.textPri }}>Learning Path Timeline</h3>
        {coursesLoading && <div className="text-xs text-center py-4 animate-pulse" style={{ color: t.textMuted }}>Loading personalised courses...</div>}
        <div className="relative">
          <div className="absolute left-4 top-0 bottom-0 w-px" style={{ background: "rgba(59,130,246,0.3)" }} />
          <div className="space-y-4">
            {courses.map((c, i) => (
              <div key={i} className="relative flex gap-4 pl-10">
                <div className="absolute left-2 w-4 h-4 rounded-full border-2 border-blue-500 flex items-center justify-center mt-1.5" style={{ background: t.bg }}>
                  <div className="w-1.5 h-1.5 rounded-full bg-blue-500" />
                </div>
                <div className="flex-1 p-4 rounded-xl" style={{ background: t.bgInput, border: `1.5px solid ${t.bgInputBdr}` }}>
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center flex-wrap gap-1.5 mb-1.5">
                        <span className="text-xs font-bold text-blue-400">Month {c.month}</span>
                        {c.free && <Pill color="green">Free</Pill>}
                        <Pill color="blue">{c.duration}</Pill>
                        <Pill color="purple">{c.provider}</Pill>
                      </div>
                      <div className="text-sm font-semibold mb-0.5" style={{ color: t.textPri }}>{c.name}</div>
                      <div className="text-xs" style={{ color: t.textMuted }}>Skill gained: <span style={{ color: t.textSec }}>{c.skill}</span></div>
                    </div>
                    <a href={c.url} target="_blank" rel="noopener noreferrer"
                      className="flex-shrink-0 flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-bold transition-all hover:scale-105"
                      style={{ background: "linear-gradient(135deg,#3b82f6,#2563eb)", color: "white", textDecoration: "none", whiteSpace: "nowrap", boxShadow: "0 2px 10px rgba(59,130,246,0.35)" }}
                      onClick={e => e.stopPropagation()}>
                      Enroll Now
                      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5">
                        <path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6M15 3h6v6M10 14L21 3" />
                      </svg>
                    </a>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </Card>

      {/* AI Replacement Simulator — API 6: GET /api/job-impact?ai_level */}
      <Card className="p-5" hover={false}>
        <h3 className="font-bold mb-1" style={{ color: t.textPri }}>AI Replacement Simulator</h3>
        <p className="text-xs mb-5" style={{ color: t.textMuted }}>Adjust AI adoption level to see predicted job impact</p>
        <div className="mb-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm" style={{ color: t.textSec }}>AI Adoption Level</span>
            <span className="text-lg font-black text-orange-400">{aiAdoption}%{jobImpactLoading ? " …" : ""}</span>
          </div>
          <input type="range" min="0" max="100" value={aiAdoption} onChange={e => setAiAdoption(+e.target.value)} className="w-full" style={{ accentColor: "#f97316" }} />
          <div className="flex justify-between text-xs mt-1" style={{ color: t.textMuted }}><span>0% (Today)</span><span>50% (2027)</span><span>100% (2030)</span></div>
        </div>
        <div className="grid grid-cols-3 gap-4 mt-4">
          {[
            { label: "BPO Jobs Remaining", val: `${bpoAt}%`, sub: `↓ ${(100 - Number(bpoAt)).toFixed(0)}% displaced`, c: "#ef4444", bg: "rgba(239,68,68,0.08)", bdr: "rgba(239,68,68,0.2)" },
            { label: "AI Engineer Roles", val: `+${aiAt}%`, sub: "↑ Growth projected", c: "#22c55e", bg: "rgba(34,197,94,0.08)", bdr: "rgba(34,197,94,0.2)" },
            { label: "Your Risk Score", val: `${displayRisk}`, sub: "If you upskill now", c: "#3b82f6", bg: "rgba(59,130,246,0.08)", bdr: "rgba(59,130,246,0.2)" },
          ].map(r => (
            <div key={r.label} className="p-4 rounded-xl text-center" style={{ background: r.bg, border: `1px solid ${r.bdr}` }}>
              <div className="text-2xl font-black" style={{ color: r.c }}>{r.val}</div>
              <div className="text-xs mt-1" style={{ color: t.textMuted }}>{r.label}</div>
              <div className="text-xs mt-0.5" style={{ color: r.c }}>{r.sub}</div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );

  return (
    <div className="max-w-2xl mx-auto">
      <div className="text-center mb-8">
        <h2 className="text-3xl font-black mb-2" style={{ color: t.textPri }}>Worker Intelligence Engine</h2>
        <p style={{ color: t.textMuted }}>Get your personalized AI risk score and career transition roadmap</p>
      </div>
      <Card className="p-6" hover={false}>
        <div className="space-y-5">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-xs font-medium block mb-1.5" style={{ color: t.textMuted }}>Job Title</label>
              <SI value={form.title} onChange={e => setForm(f => ({ ...f, title: e.target.value }))} placeholder="e.g., BPO Executive, Software Engineer" />
            </div>
            <div>
              <label className="text-xs font-medium block mb-1.5" style={{ color: t.textMuted }}>City</label>
              <SI as="select" value={form.city} onChange={e => setForm(f => ({ ...f, city: e.target.value }))}>
                {CITIES.map(c => <option key={c} value={c}>{c}</option>)}
              </SI>
            </div>
          </div>
          <div>
            <label className="text-xs font-medium block mb-1.5" style={{ color: t.textMuted }}>Years of Experience</label>
            <SI type="number" value={form.experience} onChange={e => setForm(f => ({ ...f, experience: e.target.value }))} placeholder="e.g., 3" />
          </div>
          <div>
            <label className="text-xs font-medium block mb-1.5" style={{ color: t.textMuted }}>
              Describe your daily tasks <span style={{ opacity: 0.6 }}>(100–200 words)</span>
            </label>
            <SI as="textarea" value={form.tasks} onChange={e => setForm(f => ({ ...f, tasks: e.target.value }))}
              placeholder="Describe what you do daily — customer interactions, data work, tools used, team collaboration..."
              rows={5} xStyle={{ resize: "none" }} />
            <div className="text-right text-xs mt-1" style={{ color: t.textMuted }}>{form.tasks.split(" ").filter(Boolean).length} words</div>
          </div>
          <button
            onClick={handleSubmit}
            disabled={!form.title || !form.tasks}
            className="w-full py-4 rounded-xl font-bold text-white transition-all hover:scale-[1.02] disabled:opacity-40 disabled:hover:scale-100 disabled:cursor-not-allowed"
            style={{ background: "linear-gradient(135deg,#3b82f6,#2563eb)", boxShadow: "0 0 20px rgba(59,130,246,0.3)" }}>
            Generate Career Intelligence Report →
          </button>
        </div>
      </Card>
    </div>
  );
}

// ─── Shell ────────────────────────────────────────────────────────────────────

const NAV = [
  { id: "home",   label: "Dashboard",          icon: "⊞" },
  { id: "hiring", label: "Hiring Trends",       icon: "📈" },
  { id: "skills", label: "Skills Intelligence", icon: "🧬" },
  { id: "risk",   label: "AI Vulnerability",    icon: "🛡️" },
  { id: "worker", label: "Worker Engine",       icon: "🎯" },
];

function Shell() {
  const t = useT();
  const { dark } = useTheme();
  const [page, setPage] = useState("home");
  const [workerProfile, setWorkerProfile] = useState(null);
  return (
    <div className="min-h-screen" style={{
      background: t.bg, color: t.textSec,
      fontFamily: "'Plus Jakarta Sans','Inter',system-ui,sans-serif",
      backgroundImage: dark
        ? "radial-gradient(ellipse at 20% 0%,rgba(59,130,246,0.08) 0%,transparent 50%),radial-gradient(ellipse at 80% 100%,rgba(249,115,22,0.05) 0%,transparent 50%)"
        : "none",
    }}>
      {/* Sidebar */}
      <div className="fixed left-0 top-0 bottom-0 w-56 z-40 flex flex-col"
        style={{ background: t.bgSidebar, borderRight: `1px solid ${t.border}` }}>
        <div className="px-5 py-5" style={{ borderBottom: `1px solid ${t.border}` }}>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-7 h-7 rounded-lg flex items-center justify-center text-sm" style={{ background: "linear-gradient(135deg,#3b82f6,#1d4ed8)" }}>🇮🇳</div>
            <span className="font-black text-sm" style={{ color: t.textPri }}>CareerLens</span>
          </div>
          <div className="text-xs pl-9" style={{ color: t.textMuted }}>Skills Mirage Platform</div>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV.map(item => (
            <button key={item.id} onClick={() => setPage(item.id)}
              className="w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm transition-all duration-200"
              style={{ background: page === item.id ? "rgba(59,130,246,0.15)" : "transparent", color: page === item.id ? "#60a5fa" : t.textMuted, borderLeft: page === item.id ? "2px solid #3b82f6" : "2px solid transparent", fontWeight: page === item.id ? "600" : "400" }}>
              <span>{item.icon}</span><span>{item.label}</span>
            </button>
          ))}
        </nav>
        <div className="px-4 py-4" style={{ borderTop: `1px solid ${t.border}` }}>
          <div className="flex items-center gap-2 text-xs" style={{ color: t.textMuted }}><Dot color="#22c55e" /><span>Data updated 5 min ago</span></div>
        </div>
      </div>
      {/* Main */}
      <div className="ml-56">
        <header className="sticky top-0 z-30 px-8 py-3.5 flex items-center justify-between"
          style={{ background: t.bgHeader, backdropFilter: "blur(12px)", borderBottom: `1px solid ${t.border}` }}>
          <div>
            <h1 className="font-bold text-sm" style={{ color: t.textPri }}>{NAV.find(n => n.id === page)?.label}</h1>
            <p className="text-xs" style={{ color: t.textMuted }}>India Workforce Intelligence System</p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 text-xs rounded-lg px-3 py-1.5"
              style={{ background: dark ? "rgba(255,255,255,0.05)" : "rgba(0,0,0,0.04)", border: `1px solid ${t.border}`, color: t.textMuted }}>
              <Dot color="#22c55e" /><span>Live • 5 min ago</span>
            </div>
            
            <ThemeToggle />
          </div>
        </header>
        <main className="px-8 py-6">
          {page === "home"   && <Landing go={setPage} />}
          {page === "hiring" && <HiringPage />}
          {page === "skills" && <SkillsPage />}
          {page === "risk"   && <RiskPage />}
          {page === "worker" && <WorkerPage onProfileChange={setWorkerProfile} />}
        </main>
      </div>
      <Chatbot workerProfile={workerProfile} />
    </div>
  );
}

// ─── Root ─────────────────────────────────────────────────────────────────────

export default function App() {
  const [dark, setDark] = useState(true);
  return (
    <ThemeContext.Provider value={{ dark, toggle: () => setDark(d => !d) }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800;900&display=swap');
        *, *::before, *::after { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #1e3a5f; border-radius: 2px; }

        @keyframes ticker { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }
        @keyframes ping   { 75%,100% { transform: scale(2); opacity: 0; } }
        @keyframes spin   { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        @keyframes pulse  { 0%,100% { opacity: 1; } 50% { opacity: .5; } }

        .animate-ping  { animation: ping  1s cubic-bezier(0,0,.2,1) infinite; }
        .animate-spin  { animation: spin  1s linear infinite; }
        .animate-pulse { animation: pulse 2s cubic-bezier(.4,0,.6,1) infinite; }

        input[type=range] { cursor: pointer; }
        select option { background: #1f2937; color: #f8fafc; }
      `}</style>
      <Shell />
    </ThemeContext.Provider>
  );
}
