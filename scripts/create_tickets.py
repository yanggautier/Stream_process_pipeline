import uuid
from datetime import datetime
from random import randint, choice

def generate_ticket():

    demandes = ["Demande 1", "Demande 2", "Demande 3", "Demande 4", "Demande 5", "Demande 6"]
    priorities = ["Haut", "Moyen", "Bas"]
    demande_types = ["Demande type 1", "Demande type 2", "Demande type 3", "Demande type 4"]
    
    ticket = {}
    ticket["id"] = str(uuid.uuid4())
    ticket["client_id"] = str(randint(1,1000))
    now = datetime.now()
    ticket["demande"] = choice(demandes)
    ticket["demande_type"] = choice(demande_types)
    ticket["prority"] = choice(priorities)
    ticket["create_time"] = now.isoformat()

    return ticket


