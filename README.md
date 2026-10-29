# HealthRiskAI Labs (CSC 894)

Lab files for the CSC 894 capstone. Over the semester you build **HealthRiskAI**, a pipeline that turns healthcare
claims into a patient risk score. It uses only free, open-source tools that run together on your own laptop with
Docker.

> **Synthetic data only.** Nothing in this repository contains real patient data, and you must never load real
> patient data into these labs, not even "just to test".

## Labs

| Lab | What you do | Folder |
|---|---|---|
| Lab 1 | Watch two short videos on Docker and Docker Compose | [`labs/lab01`](labs/lab01/README.md) |
| Lab 2 | Install Docker on your computer and complete a test run | [`labs/lab02`](labs/lab02/README.md) |
| Lab 3 | Start the course infrastructure and set up Kafka and Spark | [`labs/lab03`](labs/lab03/README.md) (coming soon) |

Work through the labs in order. Each lab's README has step-by-step instructions, the exact commands to type, and
what you should see after each step.

## What your computer needs

| | Minimum | Recommended |
|---|---|---|
| Operating system | Windows 10/11 (64-bit), a recent macOS, or Linux | Windows 11, macOS on Apple silicon, or Ubuntu 22.04+ |
| Memory (RAM) | 8 GB (Labs 1–3 only) | 16 GB (needed for later labs) |
| CPU | 4 cores | 4+ cores |
| Free disk space | 30 GB | 50 GB |
| Other | Virtualization turned on (Windows), internet access to download images | |

## Getting the files

You need these files on your computer from Lab 2 on. Pick one way:

- **Download ZIP (no Git needed):** on this page, click the green **Code** button, then **Download ZIP**. Unzip it
  into `Documents\csc894` and rename the `healthriskai-labs-main` folder to `healthriskai-labs`.
  [Lab 2, Part D](labs/lab02/README.md#part-d-get-the-course-files) walks you through it.
- **Git clone (recommended):** install Git from <https://git-scm.com/downloads>, then in a terminal run:

  ```bash
  git clone https://github.com/rpp5069-cpu/healthriskai-labs.git
  cd healthriskai-labs
  ```

  Later, run `git pull` inside the folder to get the next lab's files.

## Repository layout

```
healthriskai-labs/
├── labs/
│   ├── lab01/          Lab 1: preparation videos
│   ├── lab02/          Lab 2: Docker install and test run (compose.yaml, site/)
│   └── lab03/          Lab 3: coming soon
├── scripts/
│   ├── check-docker.ps1   environment check for Windows
│   └── check-docker.sh    environment check for macOS and Linux
└── docs/
    └── troubleshooting.md common problems and fixes
```

## Getting help

Check [`docs/troubleshooting.md`](docs/troubleshooting.md) first. If you're still stuck, post in the course
discussion board with your operating system, the exact command you ran, and the full error message (a screenshot
is fine).
