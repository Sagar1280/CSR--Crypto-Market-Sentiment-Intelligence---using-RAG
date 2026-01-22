import dateparser
from datetime import datetime

query = "how was bitcoin 5 days ago?"
# 'settings' helps handle "yesterday" relative to now
extracted_date = dateparser.parse("3 days ago", settings={'RELATIVE_BASE': datetime.now()})
print(extracted_date)