# Troubleshooting

Find the message you see, then follow the fix. If none of these match, post in the course discussion board with
your operating system, the command you ran, and the full error message.

## Installing Docker

**"Virtualization: Disabled" in Task Manager, or Docker Desktop says virtualization isn't enabled (Windows)**
Turn on virtualization in your computer's BIOS/UEFI (often called *Intel VT-x*, *AMD-V*, or *SVM*). Restart,
press the setup key shown on screen (often `F2`, `F10`, `Del`, or `Esc`), enable it, save, and restart. Search
"enable virtualization" plus your laptop model for exact steps.

**"WSL 2 installation is incomplete" or "Docker Desktop requires a newer WSL kernel" (Windows)**
Open PowerShell as Administrator and run `wsl --update`, then restart your computer and start Docker Desktop again.

**"Enter new UNIX username" prompt, or "Catastrophic failure ... Wsl/Service/E_UNEXPECTED" (Windows)**
This happens when `wsl --install` is run without `--no-distribution` on a computer that already has WSL: Windows
installs Ubuntu as well and asks you to create a Linux user. Docker doesn't need Ubuntu.
- If the prompt accepts a username and password and finishes, that's fine: type `exit` and carry on with Lab 2.
- If it keeps repeating *Catastrophic failure* and asking for a username again, remove the half-installed Ubuntu:
  1. Press `Ctrl + C`, then close that window.
  2. Open PowerShell **as administrator** and run, one at a time:

     ```powershell
     wsl --shutdown
     wsl --update
     wsl --unregister Ubuntu
     wsl --list --verbose
     ```

     The last command should list only `docker-desktop`. **Never** unregister `docker-desktop`; Docker runs on it.
  3. Start Docker Desktop, wait for **Engine running**, and continue with Part C in a regular PowerShell window.

**I use VirtualBox (Windows)**
Recent VirtualBox versions run alongside Docker Desktop on WSL 2. Don't run VirtualBox machines while the labs are
running; they compete for memory.

## Running commands

**`docker: command not found` (macOS/Linux) or `'docker' is not recognized` (Windows)**
Docker isn't installed yet, or the terminal was opened before the install finished. Close the terminal and open a
new one. If it still happens, reinstall Docker Desktop.

**`Cannot connect to the Docker daemon` or `error during connect ... docker_engine`**
Docker Desktop isn't running. Start it and wait for **Engine running** before trying again.
On Linux, run `sudo systemctl start docker`.

**`permission denied while trying to connect to the Docker daemon socket` (Linux)**
Run `sudo usermod -aG docker $USER`, then log out and back in.

**`docker-compose: command not found`**
Use `docker compose` with a **space**. The older `docker-compose` command is not used in this course.

**`no configuration file provided: not found`**
You're not in the folder that holds `compose.yaml`. For Lab 2, `cd` into `labs/lab02` and run `ls` to check that
`compose.yaml` is listed.

**`Bind for 127.0.0.1:8080 failed: port is already allocated`**
Something else is using port 8080, often the `lab02-web` container from Part E1. Run `docker ps` and remove any
leftover container with `docker rm -f lab02-web`. If another program uses 8080, change `8080` to `8081` in the
command or in `compose.yaml` and open <http://localhost:8081> instead.

**`Error response from daemon: Conflict. The container name "/lab02-web" is already in use`**
A container with that name already exists. Remove it with `docker rm -f lab02-web` and run the command again.

**`docker run hello-world` hangs or says it can't pull the image**
Check your internet connection. On a campus or work network that blocks Docker Hub, try another network such as
home Wi-Fi.

**`toomanyrequests: You have reached your unauthenticated pull rate limit`**
Docker Hub limits downloads for people who aren't signed in, and a shared campus network can reach that limit
quickly. Create a free account at <https://hub.docker.com>, then sign in from Docker Desktop (top-right **Sign in**)
or run `docker login`. Wait a few minutes and run the command again.

**`ls` shows a table with no file names (Windows)**
The PowerShell window is too narrow, so it drops the **Name** column. Widen or maximize the window, or run
`ls -Name` to list just the names.

**I unzipped the files but can't find `labs` or `scripts`**
GitHub's ZIP creates a folder named `healthriskai-labs-main`, and Windows sometimes nests it inside another folder
of the same name. Open the folders until you see `docs`, `labs`, and `scripts` side by side; that's the repository
folder. Rename it to `healthriskai-labs` so the lab commands match.

## Running the environment check

**`running scripts is disabled on this system` (Windows)**
Use the exact command from Lab 2, which allows this one script to run without changing your settings:
`powershell -ExecutionPolicy Bypass -File scripts\check-docker.ps1 | Tee-Object lab02-check.txt`

**`No such file or directory: scripts/check-docker.sh`**
Run it from the repository folder (the one that contains `scripts`), not from `labs/lab02`. Run `cd ../..` first.

**WARN: memory available to Docker is low**
Close other apps (browsers use a lot). On macOS, raise it in Docker Desktop > Settings > Resources.
On Windows, Docker gets half of your RAM by default. To set it yourself, create a file named `.wslconfig` in your
user folder (`C:\Users\<your name>\.wslconfig`) containing:

```ini
[wsl2]
memory=10GB
```

Then run `wsl --shutdown` in PowerShell and start Docker Desktop again. Use `10GB` on a 16 GB computer, or `6GB` on
an 8 GB computer.

**WARN: free disk space is low**
Delete large files you no longer need or empty the trash. Docker images for the later labs need about 30 GB.
