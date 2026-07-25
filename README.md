# KEDA & Kafka Autoscaling Tutorials

🔗 **Live site:** https://aw-santo.github.io/keda-tuto/

An interview-grade, self-contained course on event-driven autoscaling on Kubernetes — an index note
plus 18 numbered modules and 4 hands-on labs, with animated diagrams wherever motion helps explain
a concept. Kafka consumer autoscaling is the running example throughout.

Built with [Quartz](https://quartz.jzhao.xyz/), the same static-site generator behind the
[Spring Boot Tutorials](https://aw-santo.github.io/tutos/) site.

## Local dev

```bash
npm ci
npx quartz plugin install
npx quartz build --serve
```

## Deploy

Push to `v5` — GitHub Actions builds the site and deploys it to GitHub Pages (see
`.github/workflows/deploy.yml`).
