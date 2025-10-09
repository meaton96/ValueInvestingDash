LOG_UPLOAD = """
INSERT INTO etl_logs
  (pipeline_name, time_start, time_end, status, errors, notes)
VALUES
  (:pipeline_name, :time_start, :time_end, :status, :errors, :notes)
"""
