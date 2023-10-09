from datetime import datetime, timedelta
import  Dao.resarcher_baremaSQL as  resarcher_baremaSQL



hoje = datetime.today() - timedelta(days=5)
print(hoje.date())


print(resarcher_baremaSQL.researcher_production_db("1966167015825708;8933624812566216","2010","1900"))