"""Pull-based runner coordination layer.

Registration, heartbeat, HTTP client and active-job tracking used by the
runner process to talk to the backend runner API. No docker, no Flask.
"""
