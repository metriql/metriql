# Metriql
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-2-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

Metriql is an open-source metrics store which allows companies to define their metrics as code and share them across their BI and data tools easily.
It uses [dbt](https://getdbt.com) for the transformation layer and integrate with dbt via its [manifest.json artifact](https://docs.getdbt.com/reference/artifacts/manifest-json). 
You can learn more about metriql from [here](https://metriql.com/introduction/intro).

This is the core repository of the [metriql](http://metriql.com) project. It includes the JDBC driver, REST API, and the CLI.

## Integrations

* [metriql CLI](https://metriql.com/metriql-cli/cli-overview)
* [REST API](https://metriql.com/integrations/rest-api)
* [JDBC Driver](https://metriql.com/integrations/jdbc-driver)
* [BI tools](https://metriql.com/integrations/bi-tools/index)
* [Embedded Applications](https://metriql.com/integrations/embedded)

## Running Trino in your IDE

Run the following commands to pull Metriql and build it locally:

```
git clone https://github.com/metriql/metriql.git
cd metriql
./mvnw clean install -DskipTests
cd ./frontend && npm run build
```

We suggest using Intellij, here are the configuration that you need to run Metriql locally. Here is the required configuration:

```
Main class: com.metriql.ServiceStarterKt
Program arguments: serve --jdbc --manifest-json "https://metriql.github.io/metriql-public-demo/manifest.json"
```

Once you're with the setup, please make sure that:

* [dbt profile setup](https://docs.getdbt.com/dbt-cli/configure-your-profile) is done for Metriql to connect your database.
* [Additional dependencies](https://github.com/metriql/metriql/blob/master/Dockerfile#L27) are installed locally for Metriql to integrate with downstream tools.


## Goodreads

* [Metriql launch post](https://metriql.com/blog/introducing-metriql-open-source-metrics-store)
* [OLAP in the modern data stack](https://metriql.com/blog/2021/09/07/olap-in-modern-data-stack)
* [How we built Metriql Public Demo](https://metriql.com/blog/2021/09/29/how-we-built-our-public-demo)

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><img src="https://avatars.githubusercontent.com/u/32792779?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Hassam Saeed</b></sub><br /><a href="#data-HassamSaeed" title="Data">🔣</a></td>
    <td align="center"><a href="http://www.linkedin.com/in/umarmajeedrana"><img src="https://avatars.githubusercontent.com/u/19478456?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Umar Majeed</b></sub></a><br /><a href="https://github.com/metriql/metriql/commits?author=UmarMajeed-Rana" title="Code">💻</a> <a href="#platform-UmarMajeed-Rana" title="Packaging/porting to new platform">📦</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. All contributions are welcome!

Take a look at our open positions at the [careers page](https://metriql.com/careers) if you'd like to join our team and help us build the next big thing in the modern data stack.
