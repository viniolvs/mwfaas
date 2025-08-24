# mwfaas/cli/configure_globus_endpoints.py

from ..globus_compute_manager import GlobusComputeCloudManager


def main():
    with GlobusComputeCloudManager(auto_authenticate=True) as cloud_manager:
        cloud_manager.configure_endpoints_interactive_and_save()


if __name__ == "__main__":
    main()
