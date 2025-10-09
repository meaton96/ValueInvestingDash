LOG_UPLOAD_ALCH = """
INSERT INTO etl_logs
  (pipeline_name, time_start, time_end, status, errors, notes)
VALUES
  (:pipeline_name, :time_start, :time_end, :status, :errors, :notes)
"""

LOG_UPLOAD_PG = """
INSERT INTO etl_logs
  (pipeline_name, time_start, time_end, status, errors, notes)
VALUES
  (%(pipeline_name)s, %(time_start)s, %(time_end)s, %(status)s, %(errors)s, %(notes)s)
"""
