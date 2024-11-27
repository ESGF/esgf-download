from esgpull import Esgpull
from sqlalchemy.orm import Query

query = 'insert query sha here'
esg = Esgpull()
query=esg.graph.get("920fac")
graph = esg.graph.subgraph(query)
graph.asdict(files=True)

# todo possibly remove this