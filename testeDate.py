from datetime import datetime, timedelta



hoje = datetime.today() - timedelta(days=5)
print(hoje.date())
