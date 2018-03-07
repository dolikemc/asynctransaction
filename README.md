# asynctransaction
Framework to connect data bases of small applications (micro services) via 
a simple (REST-)API. Use aiohttp.

**Setup**

Create a config file in the root folder of the application.

`# content of likemc.ini`

`[DATABASE]`

`task_db: /<--root_folder_of_the_application-->/transaction.db`

`[SERVER]`

`task: 3010`


Create a SQLite data base where your config task_db entry points to.

`~/sqlite3 -batch transaction.db < data/model/transaction.sql`

To be continued ...




