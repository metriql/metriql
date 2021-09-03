<template>
  <h4>Installation</h4> <br>
<pre>
$ pip install trino
</pre>
<br>
You can use Trino's <a href="https://github.com/trinodb/trino-python-client">Python package</a> to access Metriql and run queries as follows:
<br><br>
<pre>
import trino
conn = trino.dbapi.connect(
    host='{{ $BASE.HOSTNAME }}',
    port={{ $BASE.PORT }},
    user='YOUR_METRIQL_USERNAME',
    catalog='metriql',
    schema='public',
    auth=trino.auth.BasicAuthentication("YOUR_METRIQL_USERNAME", "YOUR_METRIQL_PASSWORD"),
)
cur = conn.cursor()
cur.execute('SHOW TABLES')
rows = cur.fetchall()
</pre>

</template>
