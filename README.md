# Solar Consumer
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-15-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

[![ease of contribution: easy](https://img.shields.io/badge/ease%20of%20contribution:%20easy-32bd50)](https://github.com/openclimatefix#how-easy-is-it-to-get-involved)

This code can be used to download solar forecasts and save them to a PostgreSQL database. It fetches solar generation estimates for embedded solar farms and processes the data for analysis.
We currently collect
- UK: Forecast can be retreived from NESO. Generation Data can be retrevied from PVLive. 
- NL: Generation values from Ned NL, both national and region. National Forecast values from Ned NL too. 
- DE: Generation values from ENTSOE for several TSOs. 
- BE: Solar PV forecast data (national and regional) from Elia Open Data API.
- India (Rajasthan): Real-time solar and wind generation data from RUVNL (Rajasthan Urja Vikas Nigam Limited).



Here are the different sources of data, and which methods can be used to save the results

| Source | Country | CSV | Data Platform | DB (Legacy) | Site DB (Legacy) |
| --- | ---  | --- | --- | --- |  ---|
| PVLive | 🇬🇧 | ✅ | ✅ | | |
| NESO forecast | 🇬🇧 | ✅ | | ✅| 
| Ned-nl | 🇳🇱 | ✅ || | ✅ |
| Ned-nl forecast | 🇳🇱 | ✅ ||| ✅ |
| Germany (ENTSOE) | 🇩🇪 |  ✅ ||| ✅ |
| Elia Open Data | 🇧🇪 | ✅ |  |  |  |
| RUVNL (Rajasthan SLDC) | 🇮🇳 | ✅ |  |  |  |


## Requirements

- Docker
- Docker Compose

## Installation & Running

1. Clone the repository:
```bash
git clone https://github.com/openclimatefix/neso-solar-consumer.git
cd neso-solar-consumer
```

2. Copy the example environment file:
```bash
cp .example.env .env
```

3. Start the application:
```bash
docker compose up -d
```

The above command will:
- Start a PostgreSQL database container
- Build and start the NESO Solar Consumer application
- Configure all necessary networking between containers

To stop the application:
```bash
docker compose down
```

To view logs:
```bash
docker compose logs -f
```

> **Note**: The PostgreSQL data is persisted in a Docker volume. To completely reset the database, use:
> ```bash
> docker compose down -v
> ```

## Documentation

The package provides three main functionalities:

1. **Data Fetching**: Retrieves solar forecast data from the NESO API
2. **Data Formatting**: Processes the data into standardized forecast objects
3. **Data Storage**: Saves the formatted forecasts to a PostgreSQL database

### Key Components:

- `fetch_data.py`: Handles API data retrieval
- `format_forecast.py`: Converts raw data into forecast objects
- `save_forecast.py`: Manages database operations
- `app.py`: Orchestrates the entire pipeline

### Environment Variables: (Can be found in the .example.env / .env file)

- `DB_URL=postgresql://postgres:postgres@localhost:5432/neso_solar` : Database Configuration
- `COUNTRY="gb"` : Country code for fetching data. Currently, other options are ["be", "ind_rajasthan", "nl"] 
- `SAVE_METHOD="db"`: Ways to store the data. Currently other options are ["csv", "site-db"]
- `CSV_DIR=None` : Directory to save CSV files if `SAVE_METHOD="csv"`.
- `UK_PVLIVE_REGIME=in-day`: For UK PVLive, the regime. Can be "in-day" or "day-after"
- `UK_PVLIVE_N_GSPS=342`: For UK PVLive, the amount of gsps we pull data for.
- `UK_PVLIVE_BACKFILL_HOURS=2`: For UK PVLive, the amount of backfill hours we pull, when regime="in-day"
- 

## Development

1. Set up the development environment:
```bash
pip install ".[dev]"
```

2. Run tests:
```bash
pytest
```

3. Format code:
```bash
black .
```

4. Run linter:
```bash
ruff check .
```

### Running the Test Suite

The test suite includes unit tests and integration tests:

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_fetch_data.py

# Run with coverage
pytest --cov=neso_solar_consumer
```

### Continuous Integration (CI)

This reposistory has 2 main CI workflows - `branch-ci` and `merged-ci`. 

- `branch-ci` is triggered on all pushes to any branch except `main`, and on any pull request that is opened, reopened or updated. It runs the tests suite, lints the project, and builds and pushes a dev image.
- `merged-ci` is triggered on any pull request merged into `main`. It bumps the git tag, and builds and pushes a container with that tag.

## FAQ

**Q: What format is the data stored in?**
A: The data is stored in PostgreSQL using SQLAlchemy models, with timestamps in UTC and power values in megawatts.

**Q: How often should I run the consumer?**
A: This depends on your use case and the NESO API update frequency. The consumer can be scheduled using cron jobs or other scheduling tools.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing and community

[![issues badge](https://img.shields.io/github/issues/openclimatefix/neso-solar-consumer?color=FFAC5F)](https://github.com/openclimatefix/neso-solar-consumer/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)

- PR's are welcome! See the [Organisation Profile](https://github.com/openclimatefix) for details on contributing
- Find out about our other projects in the [OCF Meta Repo](https://github.com/openclimatefix/ocf-meta-repo)
- Check out the [OCF blog](https://openclimatefix.org/blog) for updates
- Follow OCF on [LinkedIn](https://uk.linkedin.com/company/open-climate-fix)


## Contributors


<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt="Peter Dudfield"/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="#ideas-peterdudfield" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Seao7"><img src="https://avatars.githubusercontent.com/u/100257888?v=4?s=100" width="100px;" alt="Seao7"/><br /><sub><b>Seao7</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=Seao7" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://siddharth7113.github.io"><img src="https://avatars.githubusercontent.com/u/114160268?v=4?s=100" width="100px;" alt="Siddharth"/><br /><sub><b>Siddharth</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/pulls?q=is%3Apr+reviewed-by%3Asiddharth7113" title="Reviewed Pull Requests">👀</a> <a href="#infra-siddharth7113" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/openclimatefix/solar-consumer/commits?author=siddharth7113" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Conor0Callaghan"><img src="https://avatars.githubusercontent.com/u/4090256?v=4?s=100" width="100px;" alt="Conor O Callaghan"/><br /><sub><b>Conor O Callaghan</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=Conor0Callaghan" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/alirashidAR"><img src="https://avatars.githubusercontent.com/u/110668489?v=4?s=100" width="100px;" alt="Ali Rashid"/><br /><sub><b>Ali Rashid</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=alirashidAR" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ManzoorAhmedShaikh"><img src="https://avatars.githubusercontent.com/u/110716002?v=4?s=100" width="100px;" alt="Manzoor Ahmed Shaikh"/><br /><sub><b>Manzoor Ahmed Shaikh</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=ManzoorAhmedShaikh" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://anaskhan.me"><img src="https://avatars.githubusercontent.com/u/83116240?v=4?s=100" width="100px;" alt="Anas Khan"/><br /><sub><b>Anas Khan</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=anxkhn" title="Documentation">📖</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/pjireland"><img src="https://avatars.githubusercontent.com/u/16693035?v=4?s=100" width="100px;" alt="Peter Ireland"/><br /><sub><b>Peter Ireland</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=pjireland" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vashisthrahul13"><img src="https://avatars.githubusercontent.com/u/182660137?v=4?s=100" width="100px;" alt="vashisthrahul13"/><br /><sub><b>vashisthrahul13</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=vashisthrahul13" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rahul-ahuja"><img src="https://avatars.githubusercontent.com/u/21355015?v=4?s=100" width="100px;" alt="rahul-ahuja"/><br /><sub><b>rahul-ahuja</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=rahul-ahuja" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://rahul-joon.github.io/My-Website/"><img src="https://avatars.githubusercontent.com/u/61495262?v=4?s=100" width="100px;" alt="Rahul Joon"/><br /><sub><b>Rahul Joon</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=Rahul-JOON" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/michael-gendy"><img src="https://avatars.githubusercontent.com/u/64384201?v=4?s=100" width="100px;" alt="michael-gendy"/><br /><sub><b>michael-gendy</b></sub></a><br /><a href="#infra-michael-gendy" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Shohail-Ismail"><img src="https://avatars.githubusercontent.com/u/149825575?v=4?s=100" width="100px;" alt="Shohail Ismail"/><br /><sub><b>Shohail Ismail</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=Shohail-Ismail" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Prafful-Vyas"><img src="https://avatars.githubusercontent.com/u/118352579?v=4?s=100" width="100px;" alt="Prafful Vyas"/><br /><sub><b>Prafful Vyas</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=Prafful-Vyas" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://www.linkedin.com/in/ram-from-tvl"><img src="https://avatars.githubusercontent.com/u/114728749?v=4?s=100" width="100px;" alt="Ramkumar R"/><br /><sub><b>Ramkumar R</b></sub></a><br /><a href="https://github.com/openclimatefix/solar-consumer/commits?author=ram-from-tvl" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
*Part of the [Open Climate Fix](https://github.com/orgs/openclimatefix/people) community.*

[![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)](https://openclimatefix.org)
