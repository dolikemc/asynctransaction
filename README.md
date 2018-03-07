# asynctransaction
Framework to connect data bases of small applications (micro services) via 
a simple (REST-)API. Use aiohttp.

**Setup**

Create a config file and a SQLite data base in the root folder where the server 
should be started.

`# content of likemc.ini`

`[DATABASE]`

`task_db: /<--root_folder_of_the_application-->/transaction.db`

`[SERVER]`

`task: 3010`


To be continued...




