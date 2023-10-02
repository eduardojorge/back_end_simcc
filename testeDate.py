from datetime import datetime, timedelta
import  Dao.resarcher_baremaSQL as  resarcher_baremaSQL



hoje = datetime.today() - timedelta(days=5)
print(hoje.date())


print(resarcher_baremaSQL.researcher_production_db("8933624812566216;8933624812566216","2010","1900"))