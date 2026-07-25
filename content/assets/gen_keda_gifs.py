"""
gen_keda_gifs.py — animated explainer GIFs for the KEDA track.

Built on the `animated-diagrams` engine (Pillow). Each build_* function writes
one GIF into this folder. Re-run to regenerate:

    python3 gen_keda_gifs.py              # all figures
    python3 gen_keda_gifs.py lagmath      # one figure by key

Figures:
  blindspot   01 - CPU stays flat while lag climbs -> HPA sees nothing, KEDA scales
  metricspath 03 - HPA -> apiserver -> metrics adapter -> gRPC -> operator -> Kafka,
                   plus the operator's separate 0<->1 write on the scale subresource
  lagmath     07 - totalLag -> partition clamp -> ceil(lag/lagThreshold) -> replicas
"""

import os, sys

# locate the engine shipped with the animated-diagrams skill
ENGINE_DIR = os.path.expanduser("~/.claude/skills/animated-diagrams")
sys.path.insert(0, ENGINE_DIR)

from engine import (
    Canvas, save_gif, eseq, ease, back, tween, mix, seg_point,
    SKY, MINT, GRAPE, TANG, PINK, TEAL, CORAL, SUN, INDIGO, GRAY,
    INK, SUB, FAINT, HAIR,
)

HERE = os.path.dirname(os.path.abspath(__file__))
def out(name): return os.path.join(HERE, name)


# ---------------------------------------------------------------------------
# 01 — The HPA blind spot: CPU flat, lag climbing
# ---------------------------------------------------------------------------
def build_blindspot():
    frames = []
    cpu_box = (70, 130, 400, 150)
    lag_box = (70, 320, 400, 150)
    dec_box = (600, 190, 370, 230)

    def base(c, title_pal):
        c.header("1", "Why CPU is the wrong signal for a Kafka consumer", title_pal)
        c.card(*cpu_box, fill=GRAY["soft"], line=GRAY["bold"])
        c.text(cpu_box[0] + 18, cpu_box[1] + 26, "CPU utilisation", size=15, weight="Semibold", fill=INK)
        c.card(*lag_box, fill=TANG["soft"], line=TANG["bold"])
        c.text(lag_box[0] + 18, lag_box[1] + 26, "Consumer group lag", size=15, weight="Semibold", fill=TANG["ink"])
        c.card(*dec_box, fill=SKY["soft"], line=SKY["bold"])

    def spark(c, box, pts, pal, label):
        # pts: list of 0..1 heights, drawn as a polyline inside box
        x0, y0, w, h = box
        gx, gy, gw, gh = x0 + 20, y0 + 52, w - 46, h - 78
        c.line(gx, gy + gh, gx + gw, gy + gh, fill=HAIR, width=1.2)
        prev = None
        for i, v in enumerate(pts):
            px = gx + gw * (i / max(1, len(pts) - 1))
            py = gy + gh - gh * v
            if prev:
                c.line(prev[0], prev[1], px, py, fill=pal["bold"], width=2.6)
            prev = (px, py)
        if prev:
            c.glow(prev[0], prev[1], pal["bold"], r=6)
            c.text(prev[0] + 12, prev[1], label, size=13, weight="Bold", fill=pal["bold"], anchor="lm")

    # SCENE 1 — lag climbs, CPU stays flat, HPA holds at 1 replica
    N = 34
    for k, t in enumerate(eseq(N)):
        c = Canvas(style="bytebytego")
        base(c, GRAY)
        n = max(2, int(2 + t * 22))
        cpu_pts = [0.30 + 0.035 * ((i * 7) % 5 - 2) for i in range(n)]
        lag_pts = [min(1.0, (i / 23.0) ** 0.85) for i in range(n)]
        spark(c, cpu_box, cpu_pts, GRAY, "~31%")
        spark(c, lag_box, lag_pts, TANG, f"{int(tween(0, 90000, t)):,}")
        c.text(dec_box[0] + dec_box[2] / 2, dec_box[1] + 34, "Plain HPA", size=17, weight="Heavy", fill=SKY["ink"], anchor="mm")
        c.text(dec_box[0] + dec_box[2] / 2, dec_box[1] + 66, "watches CPU only", size=13, fill=SUB, anchor="mm")
        c.dashed(cpu_box[0] + cpu_box[2], cpu_box[1] + cpu_box[3] / 2, dec_box[0], dec_box[1] + 90, fill=HAIR)
        c.dashed(lag_box[0] + lag_box[2], lag_box[1] + lag_box[3] / 2, dec_box[0], dec_box[1] + 150, fill=CORAL["soft"])
        c.text((lag_box[0] + lag_box[2] + dec_box[0]) / 2, lag_box[1] + 40, "not visible", size=12, weight="Bold", fill=CORAL["bold"], anchor="mm")
        c.pill(dec_box[0] + 70, dec_box[1] + 120, 230, 62, fill=GRAY["soft"], line=GRAY["bold"], width=1.6)
        c.text(dec_box[0] + 185, dec_box[1] + 151, "replicas: 1", size=20, weight="Heavy", fill=INK, anchor="mm")
        c.footer("Lag climbs to 90k. CPU never moves — so the HPA never scales.", CORAL, step=0, total=2)
        frames.append(c.img)

    # SCENE 2 — KEDA reads lag from the broker; replicas track the backlog
    for t in eseq(30):
        c = Canvas(style="bytebytego")
        base(c, MINT)
        cpu_pts = [0.30 + 0.035 * ((i * 7) % 5 - 2) for i in range(24)]
        lag_pts = [min(1.0, (i / 23.0) ** 0.85) for i in range(24)]
        drain = [lag_pts[-1] * (1 - 0.75 * ease(min(1.0, t * 1.3)))]
        spark(c, cpu_box, cpu_pts, GRAY, "~31%")
        spark(c, lag_box, lag_pts + drain * 3, TANG, f"{int(tween(90000, 24000, t)):,}")
        c.text(dec_box[0] + dec_box[2] / 2, dec_box[1] + 34, "KEDA", size=17, weight="Heavy", fill=MINT["ink"], anchor="mm")
        c.text(dec_box[0] + dec_box[2] / 2, dec_box[1] + 66, "reads lag from the broker", size=13, fill=SUB, anchor="mm")
        c.arrow(lag_box[0] + lag_box[2], lag_box[1] + lag_box[3] / 2, dec_box[0], dec_box[1] + 110, fill=MINT["bold"], width=2.2)
        reps = int(round(tween(1, 12, ease(t))))
        c.pill(dec_box[0] + 70, dec_box[1] + 120, 230, 62, fill=mix(GRAY["soft"], MINT["soft"], ease(t)), line=MINT["bold"], width=1.8)
        c.text(dec_box[0] + 185, dec_box[1] + 151, f"replicas: {reps}", size=20, weight="Heavy", fill=MINT["ink"], anchor="mm")
        c.footer("Lag is the signal that actually tracks the work waiting to be done.", MINT, step=1, total=2)
        frames.append(c.img)

    save_gif(frames, out("hpa-blindspot.gif"), frame_ms=70)


# ---------------------------------------------------------------------------
# 03 — The metrics read path, and the operator's separate 0<->1 write
# ---------------------------------------------------------------------------
def build_metricspath():
    frames = []
    hpa = (60, 128, 200, 84)
    api = (330, 128, 210, 84)
    mas = (610, 128, 220, 84)
    ope = (610, 330, 220, 96)
    kfk = (880, 330, 120, 96)
    dep = (60, 330, 200, 96)

    def base(c, pal):
        c.header("3", "Who asks whom: the KEDA metrics path", pal)
        c.card(*hpa, fill=SKY["soft"], line=SKY["bold"])
        c.text(hpa[0] + hpa[2] / 2, hpa[1] + 34, "HPA controller", size=14, weight="Semibold", fill=SKY["ink"], anchor="mm")
        c.text(hpa[0] + hpa[2] / 2, hpa[1] + 58, "kube-controller-manager", size=11, fill=SUB, anchor="mm")

        c.card(*api, fill=GRAY["soft"], line=GRAY["bold"])
        c.text(api[0] + api[2] / 2, api[1] + 32, "kube-apiserver", size=14, weight="Semibold", fill=INK, anchor="mm")
        c.text(api[0] + api[2] / 2, api[1] + 56, "aggregation layer", size=11, fill=SUB, anchor="mm")

        c.card(*mas, fill=GRAPE["soft"], line=GRAPE["bold"])
        c.text(mas[0] + mas[2] / 2, mas[1] + 30, "keda-metrics-apiserver", size=13, weight="Semibold", fill=GRAPE["ink"], anchor="mm")
        c.text(mas[0] + mas[2] / 2, mas[1] + 54, ":6443", size=11, fill=SUB, anchor="mm")

        c.card(*ope, fill=MINT["soft"], line=MINT["bold"])
        c.text(ope[0] + ope[2] / 2, ope[1] + 30, "keda-operator", size=14, weight="Semibold", fill=MINT["ink"], anchor="mm")
        c.text(ope[0] + ope[2] / 2, ope[1] + 54, "scalers + :9666", size=11, fill=SUB, anchor="mm")

        c.card(*kfk, fill=SUN["soft"], line=SUN["bold"])
        c.text(kfk[0] + kfk[2] / 2, kfk[1] + 42, "Kafka", size=14, weight="Semibold", fill=INK, anchor="mm")
        c.text(kfk[0] + kfk[2] / 2, kfk[1] + 64, "broker", size=11, fill=SUB, anchor="mm")

        c.card(*dep, fill=TEAL["soft"], line=TEAL["bold"])
        c.text(dep[0] + dep[2] / 2, dep[1] + 42, "Deployment", size=14, weight="Semibold", fill=TEAL["ink"], anchor="mm")
        c.text(dep[0] + dep[2] / 2, dep[1] + 64, "your consumer", size=11, fill=SUB, anchor="mm")

        c.line(hpa[0] + hpa[2], hpa[1] + 42, api[0], api[1] + 42, fill=HAIR)
        c.line(api[0] + api[2], api[1] + 42, mas[0], mas[1] + 42, fill=HAIR)
        c.line(mas[0] + mas[2] / 2, mas[1] + mas[3], ope[0] + ope[2] / 2, ope[1], fill=HAIR)
        c.line(ope[0] + ope[2], ope[1] + 48, kfk[0], kfk[1] + 48, fill=HAIR)

    QUERY = [((260, 170), (330, 170)), ((540, 170), (610, 170)),
             ((720, 212), (720, 330)), ((830, 378), (880, 378))]

    # SCENE 1 — the query travels out to the broker
    for t in eseq(36):
        c = Canvas(style="bytebytego")
        base(c, SKY)
        c.tag(560, 148, "external.metrics.k8s.io/v1beta1")
        c.tag(688, 268, "gRPC  mTLS  :9666")
        px, py = seg_point(QUERY, t)
        c.glow(px, py, SKY["bold"])
        c.footer("The HPA asks the apiserver; only the operator ever talks to Kafka.", SKY, step=0, total=3)
        frames.append(c.img)

    # SCENE 2 — the lag value comes back the same way
    for t in eseq(32):
        c = Canvas(style="bytebytego")
        base(c, TANG)
        c.tag(560, 148, "external.metrics.k8s.io/v1beta1")
        c.tag(688, 268, "gRPC  mTLS  :9666")
        rev = list(reversed([(b, a) for (a, b) in QUERY]))
        px, py = seg_point(rev, t)
        c.glow(px, py, TANG["bold"])
        c.text(px + 14, py - 16, "lag = 4,800", size=12, weight="Bold", fill=TANG["bold"], anchor="lm")
        c.footer("The value returns as one External metric — and the HPA does the division.", TANG, step=1, total=3)
        frames.append(c.img)

    # SCENE 3 — two different writers: HPA does 1<->N, the operator does 0<->1
    for t in eseq(34):
        c = Canvas(style="bytebytego")
        base(c, MINT)
        pulse = 0.5 + 0.5 * __import__("math").sin(t * 2 * 3.14159)
        c.arrow(hpa[0] + 60, hpa[1] + hpa[3], dep[0] + 60, dep[1], fill=mix(HAIR, SKY["bold"], pulse), width=2.4)
        c.tag(hpa[0] + 74, (hpa[1] + hpa[3] + dep[1]) / 2 - 8, "1 <-> N")
        c.arrow(ope[0], ope[1] + 70, dep[0] + dep[2], dep[1] + 70, fill=mix(HAIR, MINT["bold"], 1 - pulse), width=2.4)
        c.tag(360, dep[1] + 52, "0 <-> 1   scale subresource")
        c.footer("Two writers, one workload: the HPA scales 1<->N, the operator alone crosses zero.", MINT, step=2, total=3)
        frames.append(c.img)

    save_gif(frames, out("keda-metrics-path.gif"), frame_ms=68)


# ---------------------------------------------------------------------------
# 07 — totalLag -> partition clamp -> ceil(lag / lagThreshold) -> replicas
# ---------------------------------------------------------------------------
def build_lagmath():
    frames = []
    PARTS = 6
    THRESH = 100
    RAW = 900                      # raw summed lag across partitions
    CLAMPED = PARTS * THRESH       # 600 — what the scaler actually reports

    topic = (58, 120, 250, 330)
    clamp = (378, 168, 268, 234)
    hpabx = (716, 120, 266, 330)

    def base(c, pal):
        c.header("7", "How lag becomes a replica count", pal)
        c.card(*topic, fill=SUN["soft"], line=SUN["bold"])
        c.text(topic[0] + topic[2] / 2, topic[1] + 28, "topic  ·  6 partitions", size=13, weight="Semibold", fill=INK, anchor="mm")
        c.card(*hpabx, fill=SKY["soft"], line=SKY["bold"])
        c.text(hpabx[0] + hpabx[2] / 2, hpabx[1] + 28, "HPA controller", size=13, weight="Semibold", fill=SKY["ink"], anchor="mm")

    def bars(c, grow):
        # per-partition lag bars inside the topic card
        per = [180, 210, 120, 160, 90, 140]
        for i, v in enumerate(per):
            by = topic[1] + 56 + i * 44
            c.text(topic[0] + 18, by + 12, f"p{i}", size=11, weight="Semibold", fill=SUB)
            full = topic[2] - 76
            w = max(3, full * (v / 220.0) * grow)
            c.pill(topic[0] + 46, by, full, 22, fill="#FFFFFF", line=HAIR, width=1.0)
            c.pill(topic[0] + 46, by, w, 22, fill=TANG["bold"], line=None, width=0)
            c.text(topic[0] + 46 + full + 8, by + 12, f"{int(v * grow)}", size=10, fill=SUB, anchor="lm")

    # SCENE 1 — lag accumulates; the scaler sums it
    for t in eseq(32):
        c = Canvas(style="bytebytego")
        base(c, TANG)
        bars(c, ease(t))
        c.card(*clamp, fill=GRAY["soft"], line=GRAY["bold"])
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 40, "totalLag", size=14, weight="Semibold", fill=INK, anchor="mm")
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 96, f"{int(RAW * ease(t))}", size=40, weight="Heavy", fill=TANG["bold"], anchor="mm")
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 150, "sum of per-partition lag", size=11, fill=SUB, anchor="mm")
        c.line(topic[0] + topic[2], 285, clamp[0], 285, fill=HAIR)
        c.footer("The scaler sums per-partition lag into a single number. There is no per-partition target.", TANG, step=0, total=3)
        frames.append(c.img)

    # SCENE 2 — the clamp: allowIdleConsumers=false truncates the REPORTED metric
    for t in eseq(34):
        c = Canvas(style="bytebytego")
        base(c, CORAL)
        bars(c, 1.0)
        c.card(*clamp, fill=mix(GRAY["soft"], CORAL["soft"], ease(t)), line=CORAL["bold"])
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 34, "allowIdleConsumers: false", size=12, weight="Semibold", fill=CORAL["ink"], anchor="mm")
        shown = int(tween(RAW, CLAMPED, ease(t)))
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 96, f"{shown}", size=40, weight="Heavy", fill=CORAL["bold"], anchor="mm")
        if t > 0.25:
            c.text(clamp[0] + clamp[2] / 2, clamp[1] + 146, "partitions x lagThreshold", size=11, weight="Semibold", fill=SUB, anchor="mm")
            c.text(clamp[0] + clamp[2] / 2, clamp[1] + 170, "6 x 100 = 600", size=13, weight="Bold", fill=CORAL["bold"], anchor="mm")
        c.text(clamp[0] + clamp[2] / 2, clamp[1] - 18, "the metric is truncated, not the replica count", size=12, weight="Bold", fill=CORAL["bold"], anchor="mm")
        c.line(topic[0] + topic[2], 285, clamp[0], 285, fill=HAIR)
        c.footer("KEDA caps replicas by under-reporting the metric — so the HPA's own ceil() lands on 6.", CORAL, step=1, total=3)
        frames.append(c.img)

    # SCENE 3 — the HPA divides, and the pods appear
    for t in eseq(36):
        c = Canvas(style="bytebytego")
        base(c, MINT)
        bars(c, 1.0)
        c.card(*clamp, fill=CORAL["soft"], line=CORAL["bold"])
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 40, "reported metric", size=13, weight="Semibold", fill=CORAL["ink"], anchor="mm")
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 96, "600", size=40, weight="Heavy", fill=CORAL["bold"], anchor="mm")
        c.text(clamp[0] + clamp[2] / 2, clamp[1] + 150, "AverageValue target = 100", size=11, fill=SUB, anchor="mm")
        c.line(topic[0] + topic[2], 285, clamp[0], 285, fill=HAIR)
        c.arrow(clamp[0] + clamp[2], 285, hpabx[0], 285, fill=MINT["bold"], width=2.2)

        c.text(hpabx[0] + hpabx[2] / 2, hpabx[1] + 66, "ceil( 600 / 100 )", size=16, weight="Bold", fill=SKY["ink"], anchor="mm")
        c.text(hpabx[0] + hpabx[2] / 2, hpabx[1] + 98, "= 6 replicas", size=15, weight="Heavy", fill=MINT["bold"], anchor="mm")
        for i in range(PARTS):
            lit = back(max(0.0, min(1.0, t * 1.7 - i * 0.13)))
            if lit <= 0.01:
                continue
            cx = hpabx[0] + 52 + (i % 3) * 82
            cy = hpabx[1] + 158 + (i // 3) * 78
            c.card(cx - 32, cy - 26, 64, 52, fill=mix(SKY["soft"], MINT["soft"], min(1.0, lit)), line=MINT["bold"], width=1.6)
            c.text(cx, cy, "pod", size=11, weight="Semibold", fill=MINT["ink"], anchor="mm")
        c.footer("One consumer per partition — the ceiling that makes autoscaling Kafka bounded.", MINT, step=2, total=3)
        frames.append(c.img)

    save_gif(frames, out("kafka-lag-to-replicas.gif"), frame_ms=68)


# ---------------------------------------------------------------------------
# 02 — Rebalancing: eager stops the world, cooperative doesn't
# ---------------------------------------------------------------------------
def build_rebalance():
    import math
    frames = []
    NP = 12
    CX, CY, CW, CG = 76, 150, 66, 58     # chip x, y, width, height
    GAP = 8
    OWNERS = [SKY, MINT, GRAPE]

    def chip_x(i):
        return CX + i * (CW + GAP)

    def chips(c, owner_of, sweep=None):
        """sweep: float 0..NP — a travelling 'still consuming' highlight."""
        for i in range(NP):
            x = chip_x(i)
            pal = owner_of[i]
            if pal is None:
                c.card(x, CY, CW, CH_, fill=GRAY["soft"], line=GRAY["bold"], width=1.4)
                c.text(x + CW / 2, CY + CH_ / 2, "--", size=15, weight="Bold", fill=FAINT, anchor="mm")
                continue
            lift = 0.0
            if sweep is not None:
                d = abs(((sweep - i + NP / 2) % NP) - NP / 2)
                lift = max(0.0, 1.0 - d / 1.6)
            c.card(x, CY, CW, CH_, fill=mix(pal["soft"], pal["bold"], 0.35 * lift),
                   line=pal["bold"], width=1.6 + 1.0 * lift)
            c.text(x + CW / 2, CY + CH_ / 2, f"p{i}", size=13, weight="Semibold",
                   fill=pal["ink"], anchor="mm")
            if lift > 0.25:
                c.dot(x + CW / 2, CY + CH_ + 12, 3.2 + 1.6 * lift, pal["bold"])

    def consumers(c, n, owner_of, pulse=0.0):
        for k in range(3):
            if k >= n:
                continue
            bx = 150 + k * 280
            pal = OWNERS[k]
            newest = (k == n - 1)
            w = 1.6 + (1.4 * pulse if newest else 0.0)
            c.card(bx, 330, 200, 92, fill=pal["soft"], line=pal["bold"], width=w)
            c.text(bx + 100, 362, f"consumer-{k+1}", size=14, weight="Semibold", fill=pal["ink"], anchor="mm")
            owned = sum(1 for o in owner_of if o is pal)
            c.text(bx + 100, 392, f"{owned} partitions", size=12, fill=SUB, anchor="mm")

    def base(c, pal, title):
        c.header("2", title, pal)
        c.text(CX, 126, "12 partitions", size=12, weight="Semibold", fill=SUB)

    CH_ = CY  # placeholder so the closure sees it; reset below
    CH_ = 58

    # SCENE 1 — one consumer owns everything; sweep shows it working through them
    for t in eseq(28):
        c = Canvas(style="bytebytego")
        base(c, SKY, "One consumer owns every partition")
        owner = [SKY] * NP
        chips(c, owner, sweep=(t * NP * 1.6) % NP)
        consumers(c, 1, owner)
        c.footer("A group of one: every partition is assigned to the single member.", SKY, step=0, total=3)
        frames.append(c.img)

    # SCENE 2a — consumer 2 joins; EAGER revokes everything, staggered left to right
    for t in eseq(26):
        c = Canvas(style="bytebytego")
        base(c, CORAL, "Eager rebalance: everyone drops everything")
        cut = ease(t) * (NP + 1)
        owner = [None if i < cut else SKY for i in range(NP)]
        chips(c, owner)
        pulse = 0.5 + 0.5 * math.sin(t * 5 * math.pi)
        consumers(c, 2, owner, pulse=pulse)
        if cut > 1:
            c.text(520, 252, "consumption STOPPED", size=17, weight="Heavy",
                   fill=mix(CORAL["soft"], CORAL["bold"], pulse), anchor="mm")
        c.footer("A new member joins — with the default assignor, all partitions are revoked first.", CORAL, step=1, total=3)
        frames.append(c.img)

    # SCENE 2b — reassigned 6/6, staggered back in
    for t in eseq(24):
        c = Canvas(style="bytebytego")
        base(c, MINT, "Eager rebalance: reassigned 6 / 6")
        fill_to = ease(t) * (NP + 1)
        owner = [(MINT if i < 6 else SKY) if i < fill_to else None for i in range(NP)]
        chips(c, owner, sweep=(t * NP * 1.2) % NP if t > 0.7 else None)
        consumers(c, 2, owner)
        c.footer("Only now does consumption resume — that pause is the cost of every scale event.", MINT, step=1, total=3)
        frames.append(c.img)

    # SCENE 3 — consumer 3 joins; COOPERATIVE moves only 4, staggered, others keep working
    MOVERS = (0, 1, 6, 7)
    for t in eseq(34):
        c = Canvas(style="bytebytego")
        base(c, GRAPE, "Cooperative rebalance: only 4 partitions move")
        prog = ease(t)
        owner = []
        for i in range(NP):
            base_pal = MINT if i < 6 else SKY
            if i in MOVERS and prog > 0.25 + 0.12 * MOVERS.index(i):
                owner.append(GRAPE)
            else:
                owner.append(base_pal)
        keepers = [i for i in range(NP) if i not in MOVERS]
        sweep_pos = keepers[int((t * len(keepers) * 1.8)) % len(keepers)]
        chips(c, owner, sweep=float(sweep_pos))
        pulse = 0.5 + 0.5 * math.sin(t * 4 * math.pi)
        consumers(c, 3, owner, pulse=pulse)
        c.text(520, 258, "the other 8 never stopped consuming", size=12,
               weight="Semibold", fill=SUB, anchor="mm")
        c.footer("Cooperative assignment moves only what it must — 4/4/4, no stop-the-world pause.", GRAPE, step=2, total=3)
        frames.append(c.img)

    save_gif(frames, out("kafka-rebalance.gif"), frame_ms=70)


# ---------------------------------------------------------------------------
# 08 — Scale to zero: who owns which transition, and when
# ---------------------------------------------------------------------------
def build_scaletozero():
    frames = []
    N = 64
    GX, GY, GW, GH = 90, 120, 880, 150       # lag chart
    RX, RY, RW, RH = 90, 330, 880, 130       # replica chart
    ACT = 0.14                                # activation threshold as a fraction of full scale

    def lag_at(i):
        if i < 8:   return 0.0
        if i < 26:  return min(1.0, (i - 8) / 18.0)
        if i < 44:  return max(0.0, 1.0 - (i - 26) / 18.0)
        return 0.0

    def reps_at(i):
        if i < 10:  return 0
        if i < 14:  return 1
        if i < 28:  return min(6, 1 + int((i - 14) / 2.4))
        if i < 46:  return max(1, 6 - int((i - 28) / 3.0))
        if i < 58:  return 1
        return 0

    def base(c, pal, title):
        c.header("8", title, pal)
        c.card(GX - 14, GY - 12, GW + 28, GH + 30, fill=None, line=HAIR, width=1.0, shadow=False)
        c.text(GX, GY - 24, "consumer lag", size=12, weight="Semibold", fill=SUB)
        c.text(RX, RY - 24, "replicas", size=12, weight="Semibold", fill=SUB)
        c.line(GX, GY + GH, GX + GW, GY + GH, fill=HAIR, width=1.2)
        c.line(RX, RY + RH, RX + RW, RY + RH, fill=HAIR, width=1.2)
        ay = GY + GH - GH * ACT
        c.dashed(GX, ay, GX + GW, ay, fill=TANG["bold"], dash=8, gap=6)
        c.tag(GX + GW - 210, ay - 16, "activationLagThreshold")

    def draw_upto(c, n):
        prev = None
        for i in range(n + 1):
            px = GX + GW * (i / (N - 1.0))
            py = GY + GH - GH * lag_at(i)
            if prev:
                c.line(prev[0], prev[1], px, py, fill=TANG["bold"], width=2.6)
            prev = (px, py)
        if prev:
            c.glow(prev[0], prev[1], TANG["bold"], r=6)
        pp = None
        for i in range(n + 1):
            px = RX + RW * (i / (N - 1.0))
            py = RY + RH - (RH - 16) * (reps_at(i) / 6.0)
            if pp:
                c.line(pp[0], pp[1], px, pp[1], fill=SKY["bold"], width=2.6)
                c.line(px, pp[1], px, py, fill=SKY["bold"], width=2.6)
            pp = (px, py)
        if pp:
            c.glow(pp[0], pp[1], SKY["bold"], r=6)
            c.text(pp[0] + 12, pp[1] - 4, f"{reps_at(n)}", size=15, weight="Heavy", fill=SKY["bold"], anchor="lm")

    SCENES = [
        (14, GRAY, "Idle at zero", "No lag, no pods, no cost. The HPA cannot express this state at all.", None),
        (30, MINT, "0 -> 1  ·  KEDA activates", "Lag crosses the activation threshold and KEDA's operator starts the first pod.", "KEDA operator"),
        (44, SKY, "1 -> N  ·  the HPA scales", "Above one replica the HPA takes over, dividing lag by lagThreshold.", "HPA"),
        (63, MINT, "1 -> 0  ·  cooldownPeriod", "Lag is gone, but KEDA waits out cooldownPeriod before removing the last pod.", "KEDA operator"),
    ]
    for si, (upto, pal, title, cap, owner) in enumerate(SCENES):
        start = 0 if si == 0 else SCENES[si - 1][0]
        for t in eseq(26):
            c = Canvas(style="bytebytego")
            base(c, pal, title)
            n = int(round(tween(start, upto, ease(t))))
            draw_upto(c, n)
            if owner:
                c.pill(GX + GW - 214, RY + RH + 26, 214, 34, fill=pal["soft"], line=pal["bold"], width=1.6)
                c.text(GX + GW - 107, RY + RH + 43, f"owned by: {owner}", size=12, weight="Bold", fill=pal["ink"], anchor="mm")
            c.footer(cap, pal, step=si, total=4)
            frames.append(c.img)

    save_gif(frames, out("scale-to-zero.gif"), frame_ms=62)


# ---------------------------------------------------------------------------
# 10 — cooldownPeriod vs stabilizationWindowSeconds: different transitions
# ---------------------------------------------------------------------------
def build_cooldown():
    import math
    frames = []
    STEPS = [0, 1, 2, 3, 4, 5, 6]
    BX, BY, BW, BH = 120, 200, 108, 74

    keda = (110, 372, 330, 96)
    hpa = (600, 372, 330, 96)

    def base(c, pal, active):
        c.header("10", "Which knob governs which transition", pal)
        for i, s in enumerate(STEPS):
            x = BX + i * (BW + 8)
            on = (i == active)
            c.card(x, BY, BW, BH, fill=SKY["soft"] if on else "#FFFFFF",
                   line=SKY["bold"] if on else HAIR, width=2.2 if on else 1.2)
            c.text(x + BW / 2, BY + BH / 2, str(s), size=22 if on else 18,
                   weight="Heavy" if on else "Medium", fill=SKY["ink"] if on else FAINT, anchor="mm")
        c.text(BX, BY - 26, "replicas", size=12, weight="Semibold", fill=SUB)

    def owners(c, which, pulse=0.0):
        for box, pal, name, knob in (
            (keda, MINT, "KEDA operator", "activation  ·  cooldownPeriod"),
            (hpa, GRAPE, "HPA controller", "behavior  ·  stabilizationWindow"),
        ):
            on = (which == name)
            c.card(*box, fill=pal["soft"] if on else GRAY["soft"],
                   line=pal["bold"] if on else HAIR,
                   width=(1.8 + 1.6 * pulse) if on else 1.2)
            c.text(box[0] + box[2] / 2, box[1] + 34, name, size=15, weight="Semibold",
                   fill=pal["ink"] if on else FAINT, anchor="mm")
            c.text(box[0] + box[2] / 2, box[1] + 64, knob, size=12,
                   fill=SUB if on else FAINT, anchor="mm")

    SCENES = [
        (0, 1, MINT, "KEDA operator", "0 -> 1", "Waking from zero is KEDA's job — gated by the trigger's activation threshold."),
        (1, 5, GRAPE, "HPA controller", "1 -> N", "Above one replica, the HPA does the arithmetic and applies its scale-up policy."),
        (5, 1, GRAPE, "HPA controller", "N -> 1", "Scaling back DOWN to one is STILL the HPA — scaleDown.stabilizationWindowSeconds, default 300s."),
        (1, 0, MINT, "KEDA operator", "1 -> 0", "Only the last step to zero is KEDA's cooldownPeriod — the knob everyone reaches for by mistake."),
    ]
    for si, (a, b, pal, owner, label, cap) in enumerate(SCENES):
        for t in eseq(30):
            c = Canvas(style="bytebytego")
            e = ease(t)
            pos = tween(a, b, e)              # continuous, so every frame differs
            cur = int(round(pos))
            base(c, pal, cur)
            pulse = 0.5 + 0.5 * math.sin(t * 3 * math.pi)
            owners(c, owner, pulse)
            ax = BX + a * (BW + 8) + BW / 2
            bx = BX + b * (BW + 8) + BW / 2
            ty = BY + BH + 26
            c.line(ax, ty, bx, ty, fill=HAIR, width=1.4)
            c.arrow(ax, ty, bx, ty, fill=pal["bold"], width=2.4)
            # the travelling token — this is what keeps the frames distinct
            tx = BX + pos * (BW + 8) + BW / 2
            c.glow(tx, ty, pal["bold"], r=7)
            c.tag((ax + bx) / 2 - 22, ty + 24, label)
            # a link from the owning controller up to the token
            oc = (keda if owner == "KEDA operator" else hpa)
            c.dashed(oc[0] + oc[2] / 2, oc[1], tx, ty + 12,
                     fill=mix(HAIR, pal["bold"], 0.4 + 0.6 * pulse), dash=7, gap=6)
            c.footer(cap, pal, step=si, total=4)
            frames.append(c.img)

    save_gif(frames, out("cooldown-vs-stabilization.gif"), frame_ms=66)


BUILDERS = {
    "blindspot": build_blindspot,
    "metricspath": build_metricspath,
    "lagmath": build_lagmath,
    "rebalance": build_rebalance,
    "scaletozero": build_scaletozero,
    "cooldown": build_cooldown,
}

if __name__ == "__main__":
    keys = sys.argv[1:] or list(BUILDERS)
    for k in keys:
        if k not in BUILDERS:
            print(f"unknown figure: {k}; options: {', '.join(BUILDERS)}"); continue
        print(f"building {k} ...")
        BUILDERS[k]()
    print("done.")
