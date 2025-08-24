# mwfaas/cli/configure_globus_endpoints.py

import sys

from ..globus_compute_manager import GlobusComputeCloudManager


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "login":
        GlobusComputeCloudManager.login_interactive()
    elif len(sys.argv) > 1 and sys.argv[1] == "logout":
        GlobusComputeCloudManager.logout()
    else:
        print("Uso: python3 -m mwfaas.cli.configure_globus_endpoints [login|logout]")
