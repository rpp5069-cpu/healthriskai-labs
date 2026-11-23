# Lab 2: Install Docker and complete a test run

By the end of this lab, Docker runs on your computer, you've started and stopped containers yourself, and an
environment check confirms you're ready for Lab 3, where you start the course infrastructure.

**Time:** about 60–90 minutes, most of it waiting for downloads.
**You need:** an internet connection and permission to install software on your computer.

Each step shows the **command** to type and **what you should see**. Type commands one at a time and wait for each
to finish. If something doesn't match, stop and check [Troubleshooting](../../docs/troubleshooting.md).

---

## Part A. Watch the preparation videos (about 75 minutes)

1. **Docker Crash Course for Absolute Beginners** (TechWorld with Nana)
   <https://www.youtube.com/watch?v=pg19Z8LL06w>
   What containers and images are, how they differ from virtual machines, running containers (ports, names, logs),
   and building an image with a Dockerfile.
2. **Kafka Crash Course – Hands-On Project** (TechWorld with Nana)
   <https://www.youtube.com/watch?v=B7CwU_tNYIE>
   Kafka's core ideas (topics, partitions, producers, consumers) and running Kafka with Docker Compose.
3. **Kafka with KRaft setup using Docker Compose** (Nanthakumaran Senthilnathan)
   <https://www.youtube.com/watch?v=aTl2iSCynVc>
   Running Kafka in KRaft mode, without ZooKeeper, which is how Kafka runs in our course.

> These are third-party videos and may use different versions than this course. When they differ, follow these
> lab instructions.

---

## Part B. Install Docker

Follow **only** the section for your operating system.

### Windows 10 or 11

**B1. Check that virtualization is on.**
Press `Ctrl + Shift + Esc` to open Task Manager, click **Performance**, then **CPU**. Under the graph, look for
**Virtualization: Enabled**.
If it says *Disabled*, you need to turn it on in your computer's BIOS/UEFI settings (often called *Intel VT-x*,
*AMD-V*, or *SVM*). Search "enable virtualization" plus your laptop model for the exact steps.

**B2. Install WSL 2.**
Open the Start menu, type **PowerShell**, right-click **Windows PowerShell**, and choose **Run as administrator**.
First check whether WSL is already installed:

```powershell
wsl --status
```

- **If it prints settings** such as a default version, WSL is already installed. Run `wsl --update`, then go to B3.
- **If it says WSL isn't installed** (or shows a help message), install it:

  ```powershell
  wsl --install --no-distribution
  ```

  You should see it download and install components. **Restart your computer** when it finishes.

> Type the command exactly as shown, including `--no-distribution`. Docker doesn't need a separate Linux system,
> and without that option Windows also installs Ubuntu and asks you to create a Linux username. If you already see
> an *Enter new UNIX username* prompt, see [Troubleshooting](../../docs/troubleshooting.md#installing-docker).

**B3. Install Docker Desktop.**
1. Go to <https://www.docker.com/products/docker-desktop/> and download **Docker Desktop for Windows**.
2. Run the installer. When asked, keep **Use WSL 2 instead of Hyper-V** checked.
3. Click **Close** when it finishes, and restart if it asks you to.

**B4. Start Docker Desktop.**
Open **Docker Desktop** from the Start menu. Accept the service agreement. Signing in is optional; you can skip
it. Wait until the bottom-left corner says **Engine running** (this can take a minute the first time).

> **Use PowerShell, not Command Prompt**, for every command in this course. From here on, open a **regular**
> PowerShell window (not *Run as administrator*); you only needed administrator rights for B2. Open a *new* window
> after installing Docker so it can find the `docker` command.

### macOS

**B1. Check your chip.** Click the Apple menu > **About This Mac**. Note whether it says **Apple M1/M2/M3/M4**
(Apple silicon) or **Intel**.

**B2. Install Docker Desktop.**
1. Go to <https://www.docker.com/products/docker-desktop/> and download Docker Desktop for **your chip**.
2. Open the downloaded `.dmg` file and drag **Docker** into **Applications**.
3. Open **Docker** from Applications. Approve the permission prompts and accept the service agreement. Signing in
   is optional.
4. Wait until the Docker whale icon in the menu bar stops animating and Docker Desktop shows **Engine running**.

**B3. Give Docker enough memory.** In Docker Desktop, open **Settings** (gear icon) > **Resources**. If your Mac
has 16 GB of RAM, set **Memory** to at least **8 GB**. Click **Apply & restart**.

Use the **Terminal** app for every command in this course.

### Linux (Ubuntu or similar)

1. Install Docker Engine and the Compose plugin by following the official guide for your distribution:
   <https://docs.docker.com/engine/install/>
2. Let your user run Docker without `sudo`, then **log out and back in**:

   ```bash
   sudo usermod -aG docker $USER
   ```

---

## Part C. Check the installation

Open a **new** terminal (PowerShell on Windows, Terminal on macOS/Linux) and run each command.

**C1. Docker version**

```bash
docker --version
```

You should see something like `Docker version 29.x.x, build ...`.

**C2. Docker Compose version**

```bash
docker compose version
```

You should see `Docker Compose version v2.30` or newer (any v5.x is also fine).
Note there is a **space** in `docker compose`, not a hyphen.

**C3. Your first container**

```bash
docker run hello-world
```

Docker downloads a tiny test image and runs it. You should see **Hello from Docker!** followed by a short
explanation of what just happened.

---

## Part D. Get the course files

You need this repository on your computer for the rest of the lab. These steps put it in a folder named
`csc894` in your Documents folder.

> **OneDrive users (Windows):** if your Documents folder is synced by OneDrive (its path contains `OneDrive`), use
> `$HOME\csc894` instead of `$HOME\Documents\csc894` in every command below. OneDrive can lock files that Docker
> is reading.

**D1. Make the course folder.**

```powershell
# Windows
mkdir $HOME\Documents\csc894
```

```bash
# macOS / Linux
mkdir -p ~/Documents/csc894
```

**D2. Download the files.** Choose one option:

- **Option 1, Download ZIP (no Git needed):**
  1. Open <https://github.com/rpp5069-cpu/healthriskai-labs>, click the green **Code** button, then **Download
     ZIP**. It saves `healthriskai-labs-main.zip` to your Downloads folder.
  2. Unzip it into the `csc894` folder. On Windows, right-click the ZIP, choose **Extract All…**, set the
     destination to `C:\Users\<your name>\Documents\csc894`, and click **Extract**. On macOS, double-click the ZIP
     and move the resulting folder into `Documents/csc894`.
  3. GitHub adds `-main` to the folder name. Rename it to `healthriskai-labs`:

     ```powershell
     # Windows
     cd $HOME\Documents\csc894
     Rename-Item healthriskai-labs-main healthriskai-labs
     ```

     ```bash
     # macOS / Linux
     cd ~/Documents/csc894
     mv healthriskai-labs-main healthriskai-labs
     ```

- **Option 2, Git (recommended for later labs):** install Git from <https://git-scm.com/downloads>, then run:

  ```bash
  cd ~/Documents/csc894
  git clone https://github.com/rpp5069-cpu/healthriskai-labs.git
  ```

  (On Windows PowerShell, `~` works too, or use `cd $HOME\Documents\csc894`.) Git names the folder
  `healthriskai-labs` for you, so there's nothing to rename.

**D3. Check the files.** Move into the repository folder and list what's there:

```powershell
cd $HOME\Documents\csc894\healthriskai-labs     # macOS / Linux: cd ~/Documents/csc894/healthriskai-labs
ls -Name                                        # macOS / Linux: ls
```

You should see `docs`, `labs`, `scripts`, and `README.md`. If you see only another folder named
`healthriskai-labs-main`, the ZIP was unzipped one level too deep: run `cd healthriskai-labs-main` and check again.

> If `ls` shows a table with an empty **Name** column on Windows, the window is too narrow. Widen it, or use
> `ls -Name` as above.

**D4. Move into the Lab 2 folder.**

```powershell
cd labs\lab02        # macOS / Linux: cd labs/lab02
ls -Name             # macOS / Linux: ls
```

You should see `compose.yaml`, `README.md`, and a `site` folder.

---

## Part E. Test run

### E1. Work with one container

**Start a web server container in the background:**

```bash
docker run -d --name lab02-web -p 127.0.0.1:8080:80 nginx:1.28-alpine
```

- `-d` runs it in the background ("detached").
- `--name lab02-web` gives it a name you can use in later commands.
- `-p 127.0.0.1:8080:80` connects port 8080 on your computer to port 80 inside the container, reachable only from
  your own computer.

You should see a long container ID.

**Open it in your browser:** go to <http://localhost:8080>. You should see **Welcome to nginx!** Refresh the
page once or twice; the next step shows those visits.

**Look at what's running:**

```bash
docker ps
```

You should see one row for `lab02-web` with status `Up`.

**Read its logs:**

```bash
docker logs lab02-web
```

You should see a few start-up lines, then one line containing `"GET / HTTP/1.1" 200` for each time your browser
loaded the page. No `GET` lines? Open <http://localhost:8080> again and rerun the command.

**See how much memory and CPU it uses:**

```bash
docker stats --no-stream lab02-web
```

You should see one row with its CPU %, memory use, and limit. (Without `--no-stream` it keeps updating; press
`Ctrl + C` to stop it.)

**List the images you've downloaded:**

```bash
docker images
```

You should see `hello-world` and `nginx`.

**Stop and remove the container:**

```bash
docker stop lab02-web
docker rm lab02-web
```

Run `docker ps -a`. `lab02-web` should no longer be listed.

> You'll probably still see the **hello-world** container from Part C, with status `Exited (0)` and a random name
> such as `strange_poitras`. That's expected: a stopped container stays listed until you remove it. Remove all
> stopped containers with `docker container prune` and type `y` to confirm.

### E2. Run two containers together with Docker Compose

The file `compose.yaml` in this folder defines two containers:

- **web** serves the page in the `site` folder.
- **checker** waits until web is healthy, fetches the page over a private network, prints a line, and exits.

This is the same idea the course infrastructure uses in Lab 3, just with two services instead of many.

**Start both** (from the `labs/lab02` folder):

```bash
docker compose up -d
```

You should see the network and both containers created, and `lab02-web-1` reported as **Healthy**.

**Check their status:**

```bash
docker compose ps -a
```

`web` should be `Up (healthy)`; `checker` should be `Exited (0)`, which means it finished successfully.

**Read the checker's output:**

```bash
docker compose logs checker
```

You should see **checker: fetching http://web/ over the lab02 network** followed by **Docker is working on this
computer.** The checker reached the web container by its service name, `web`, over the private network.

**Open the page:** go to <http://localhost:8080>. You should see **Docker is working on this computer.**

### E3. See how a mounted folder works

The web container reads its page straight from the `site` folder on your computer. Compose mounts **only that
folder**, and read-only. That's a safe habit: never mount your whole drive (for example `C:\`) into a container.

1. Open `site/index.html` in a text editor. On Windows run `notepad site\index.html`; on macOS run
   `open -e site/index.html`; or use VS Code.
2. Find the line `<h1>Docker is working on this computer.</h1>`. Change the text between the tags, for example to
   `Docker is working on my laptop!`, and save the file.
3. Refresh <http://localhost:8080>. You should see your new heading straight away, without restarting anything.

### E4. Stop everything

```bash
docker compose down
```

You should see both containers and the network removed. Run `docker compose ps -a`; it should list nothing.

---

## Part F. Run the environment check

The check runs from the **repository folder**, `healthriskai-labs`, the one that contains `docs`, `labs`, and
`scripts`. That's two levels up from `labs/lab02`. It saves its output to `lab02-check.txt`.

**Windows (PowerShell):**

```powershell
cd $HOME\Documents\csc894\healthriskai-labs
ls -Name
```

You should see `docs`, `labs`, `scripts`, and `README.md`. Maximize the window so nothing is cut off, then run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\check-docker.ps1 | Tee-Object lab02-check.txt
```

**macOS / Linux:**

```bash
cd ~/Documents/csc894/healthriskai-labs
ls
bash scripts/check-docker.sh | tee lab02-check.txt
```

You should see a list of **PASS** lines ending with **Your computer is ready for Lab 3.**

- **WARN** lines are advice, for example a low memory setting. You can still continue.
- **FAIL** lines must be fixed before Lab 3. Each one tells you what to do, and
  [Troubleshooting](../../docs/troubleshooting.md) has more detail.

---

## What to submit

1. `lab02-check.txt` from Part F, with no FAIL lines.
2. A screenshot of <http://localhost:8080> showing **your edited heading** from Part E3.

---

## Command summary

| Command | What it does |
|---|---|
| `docker --version` / `docker compose version` | Show the installed versions |
| `docker run hello-world` | Run Docker's test image |
| `docker run -d --name NAME -p 127.0.0.1:8080:80 IMAGE` | Start a container in the background with a port |
| `docker ps` / `docker ps -a` | List running / all containers |
| `docker logs NAME` | Show a container's output |
| `docker stats --no-stream NAME` | Show a container's memory and CPU use once |
| `docker container prune` | Remove all stopped containers |
| `docker images` | List downloaded images |
| `docker stop NAME` / `docker rm NAME` | Stop / remove a container |
| `docker compose up -d` | Start every service in `compose.yaml` |
| `docker compose ps -a` | List this project's containers |
| `docker compose logs SERVICE` | Show one service's output |
| `docker compose down` | Stop and remove this project's containers and network |

Next: Lab 3.
