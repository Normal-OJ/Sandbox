"""Pull-based runner coordination layer.

Registration, heartbeat, HTTP client, active-job tracking, job polling and
result sending used by the runner process to talk to the backend runner API.
Job prep reuses the dispatcher's file/testdata helpers; still no docker and
no Flask in this package.
"""
