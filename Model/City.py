class City(object):
    def __init__(
        self,
        id: str = None,
        name: str = None,
        country_id: str = None,
        state_id: str = None,
    ):
        self.id = id
        self.name = name
        self.country_id = country_id
        self.state_id = state_id

    def getJson(self):
        city = {
            "id": self.id,
            "name": self.name,
            "country": self.country_id,
            "state_id": self.state_id,
        }
        return city


if __name__ == "__main__":
    city = City(id="Jordan", name="Betoven", country_id="Love")
    print(city.getJson())
