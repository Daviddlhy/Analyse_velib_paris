# Analyse_velib_paris

# Introduction 
Hello I am a user of "Velib" in Paris. 
And sometimes, when I need to go to the office I hope to get a bike at the velib station.
But I noticed that it's not always possible.


# The Data
For my project I use the velib API.
You should find documentation [here](https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole).
The api give us the information about stations.

# Steps 

- Query the APi and preprocessing the data. 
- Store the data transformed into a database like MySQl/ postgresql.
- Execute the script all 30 min, with a crontab. 
- Analyze the data with a dashboard or a web app using streamlit.
