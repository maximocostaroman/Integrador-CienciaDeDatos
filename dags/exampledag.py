from airflow.sdk.definitions.asset import Asset
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import dag, task
from pendulum import datetime
from datetime import datetime, timedelta
import requests



default_args = {
    'owner': 'M Costa y J Lorenzo',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}


def snapshot_meta():
    a

with DAG(
    dag_id='recolectar_datos_vuelos',
    description='DAG ETL que recolecta datos de los vuelos',
    default_args=default_args,
    schedule=None, #ACA SEGURO TENEMOS QUE POENR CAD CUANTO
    catchup=False,
    #tags=['pokemon', 'parallel', 'etl']
) as dag:
    snapshot_meta


    # Nuevo DAG resultante
    fetch_pokemon_list >> [download_a, download_b] >> merge_transform >> export_logs >> enviar_correo_manual








@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    @task(
        outlets=[Asset("current_astronauts")]
    )  
    def get_astronauts(**context) -> list[dict]:

        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Oleg Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
    )


# Instantiate the DAG
example_astronauts()
