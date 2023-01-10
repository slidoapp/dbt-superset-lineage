# dbt-superset-lineage

<a href="https://github.com/slidoapp/dbt-superset-lineage/blob/main/LICENSE.md"><img alt="License: MIT" src="https://img.shields.io/github/license/slidoapp/dbt-superset-lineage"></a>
<a href="https://pypi.org/project/dbt-coverage/"><img alt="PyPI" src="https://img.shields.io/pypi/v/dbt-superset-lineage"></a>
![GitHub last commit](https://img.shields.io/github/last-commit/slidoapp/dbt-superset-lineage)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt-superset-lineage)
![PyPI - Format](https://img.shields.io/pypi/format/dbt-superset-lineage)

![dbt-superset-lineage](assets/lineage_white.png)

_Make [dbt](https://github.com/dbt-labs/dbt) docs and [Apache Superset](https://github.com/apache/superset) talk to one another_

## Why do I need something like this?
Odds are rather high that you use dbt together with a visualisation tool. If so, these questions might have popped
into your head time to time:
- "Could I get rid of this model? Does it get used for some dashboards? And in which ones, if yes?"
- "It would be so handy to see all these well-maintained column descriptions when exploring and creating charts."

In case your visualisation tool of choice is Supserset, you are in luck!

Using `dbt-superset-lineage`, you can:
- Add dependencies of Superset dashboards to your dbt sources and models
- Sync column descriptions from dbt docs to Superset

This will help you:
- Avoid broken dashboards because of deprecated or changed models
- Choosing the right attributes without navigating back and forth between chart and documentation

## Demo
The package was presented during [Coalesce](https://coalesce.getdbt.com/), the annual dbt conference, as a part of the talk
[_From 100 spreadsheets to 100 data analysts: the story of dbt at Slido_](https://www.getdbt.com/coalesce-2021/from-spreadsheets-to-data-analysts-the-story-of-dbt-at-slido/).
Watch a demo in the video below.

[![Demo video](assets/demo.png)](https://youtu.be/YA0yqYSs9BQ?t=1240)

## Installation

```
pip install dbt-superset-lineage
```

## Usage
`dbt-superset-lineage` comes with two basic commands: `pull-dashboards` and `push-descriptions`.
The documentation for the individual commands can be shown by using the `--help` option.

It includes a wrapper for [Superset API](https://superset.apache.org/docs/rest-api), one only needs to provide
`SUPERSET_ACCESS_TOKEN`/`SUPERSET_REFRESH_TOKEN` (obtained via `/security/login`)
as environment variable or through `--superset-access-token`/`superset-refresh-token` option.

**N.B.**
- Make sure to run `dbt compile` (or `dbt run`) against the production profile, not your development profile  
- In case more databases are used within dbt and/or Superset and there are duplicate names (`schema + table`) across
  them, specify the database through `--dbt-db-name` and/or `--superset-db-id` options
- Currently, `PUT` requests are only supported if CSRF tokens are disabled in Superset (`WTF_CSRF_ENABLED=False`).
- Tested on dbt v0.20.0 and Apache Superset v1.3.0. Other versions, esp. those newer of Superset, might face errors due
  to different underlying code and API.

### Pull dashboards
Pull dashboards from Superset and add them as
[exposures](https://docs.getdbt.com/docs/building-a-dbt-project/exposures/) to dbt docs with
references to dbt sources and models, making them visible both separately and as dependencies.

**N.B.**
- Only published dashboards are extracted.

```console
$ cd jaffle_shop
$ dbt compile  # Compile project to create manifest.json
$ export SUPERSET_ACCESS_TOKEN=<TOKEN>
$ dbt-superset-lineage pull-dashboards https://mysuperset.mycompany.com  # Pull dashboards from Superset to /models/exposures/superset_dashboards.yml
$ dbt docs generate # Generate dbt docs
$ dbt docs serve # Serve dbt docs
```

![Separate exposure in dbt docs](assets/exposures_1.png)

![Referenced exposure in dbt docs](assets/exposures_2.png)

### Push descriptions
Push column descriptions from your dbt docs to Superset as plain text so that they could be viewed
in Superset when creating charts.

**N.B.**:
- Run carefully as this rewrites your datasets using merged column metadata from Superset and dbt docs.
- Descriptions are rendered as plain text, hence no markdown syntax, incl. links, will be displayed.
- Avoid special characters and strings in your dbt docs, e.g. `→` or `<null>`.


```console
$ cd jaffle_shop
$ dbt compile  # Compile project to create manifest.json
$ export SUPERSET_ACCESS_TOKEN=<TOKEN>
$ dbt-superset-lineage push-descriptions https://mysuperset.mycompany.com  # Push descrptions from dbt docs to Superset
```
![Column descriptions in Superset](assets/descriptions.png)

Alternatively to providing the environment variable `SUPERSET_ACCESS_TOKEN` you may also provide the pair of 
`SUPERSET_USER` and `SUPERSET_PASSWORD` as evnironment variables.
This way `dbt-superset-lineage` will perform the login by itself.

#### Debugging

If the command line option `--superset-debug-dir </path/to/existing/directory>` is specified, 
a bunch of JSON files will be created and put into the provided directory.
These files may be helpful for debugging any unwanted behavior.

It is also useful to keep a copy of these files, e.g., on a cloud storage, when including
`dbt-superset-lineage` in an automated deployment workflow, as these files also encompass a
backup of the dataset/column configurations at the state _before_ `dbt-superset-lineage`
had modified them.  
A _restore_ functionality is not yet implemented, though.

#### Model settings

A bunch of (special) fields of the dbt models' YAML files are evaluated by `dbt-superset-lineage`.
This can be explained best by virtue of an example YAML file:

```yaml
version: 2

models:
  - name: my_model

    # The description will be transferred to the dataset description,
    # but any markdown formatting will be stripped:
    description: '{{ doc("my_model") }}'

    meta:
      
      # The `model_maturity` will be appended to the `certification.details`,
      # but only if `certification.certified_by` is set.
      model_maturity: medium # e.g.: low/medium/high

      # The `certification` will be placed in the dataset's `extra` field and 
      # thus displayed as a certification badge next to the dataset's name.
      certification:
        certified_by: Business Intelligence Team
        details: dbt-managed model

      # Provide Superset's internal user IDs for each owner of the dataset.
      owners:
        - 2 # Kevin
        - 3 # Martha

      # Note:
      # It is often useful to globally set the attributes above in `dbt_project.yml`
      # (see below) and only include it in the dataset's configuration (here)
      # for overriding the global configuration.


      # The settings in the `bi_integration` node are best kept in each model and
      # not in the `dbt_project.yaml`:
      bi_integration:

        # Whether or not this model should be automatically registered in Superset
        # if it does not exist there already:
        auto_register: true

        # Should manual editing of the dataset be prohibited in the BI tool?
        # This property controls Supersets (hidden) `is_managed_externally` flag.
        prohibit_manual_editing: true

        # The temporal column that should be used by default.
        # In Superset's API this is the `main_dttm_col` property.
        main_timestamp_column: occurred_at_date

        # These settings control the automatic population of filter values
        # based on DISTINCT queries:
        filter_value_extraction:
          # Enable/disable this feature for this dataset.
          # In Superset's API this field is called `filter_select_enabled`.
          enable: true
          # The predicate to be applied for aforementioned DISTINCT values queries:
          # In Superset's API this field is called `fetch_values_predicate`
          where: occurred_at_> current_timestamp - interval '1' year
        
        # The cache timeout for query results based on this dataset.
        # In Superset's API this property is called `cache_timeout`:
        results_cache_timeout_seconds: 86400

        # Use this property for optionally providing a warning message or a
        # usage note as markdown-fromatted text.
        # This will result in a warning symbol next to the dataset's name and
        # the rendered markdown text will be shown on a mouse-over action.
        # In Superset's API this field is equally called `warning_markdown`. 
        warning_markdown: >
          1. To achieve correct results, any query _must_...
              * either **filter on a single classification_l1 value**
              * or **group by classification_l1**.
          2. Ensure to use `sum(mrm_count)` to count the MRMs per classification.
```

As stated above, it often is useful to set some of the `meta` fields _globally_
on a folder/schema level by means of the `dbt_project.yml`. E.g.:

```yaml
models:
  my_project:

    my_folder:
      schema: my_schema_name
      +meta:
        # BI integration setings (Superset): override in model YAMLs, if needed:

        # The `model_maturity` will be appended to the `certification.details`,
        # but only if `certification.certified_by` is set:
        model_maturity: high # e.g.: low/medium/high

        # The `certification` will be placed in the dataset's `extra` field and 
        # thus displayed as a certification badge next to the dataset's name.
        certification:
          certified_by: Business Intelligence Team
          details: dbt-managed model

        # Provide Superset's internal user IDs for each owner of the dataset:
        owners:
          - 2 # Kevin
          - 3 # Martha
          - 4 # Bruno
          - 5 # Philipp
```


#### Column settings

In analogy to the model settings, these are the _column_ properties that are 
evaluated by `dbt-superset-lineage`:

```yaml
version: 2

models:
  - name: my_model
    description: ...
    meta:
      ...

    columns:

    - name: column_1
      description: >
        This is the column's detailed description.
        It will be carried over to Superset's column description,
        but any markdown formatting will be stripped.

      meta:

        # dbt has no native concept of verbose names, so we place a
        # `verbose_name` property in `meta`.
        # If no `verbose_name` property is defined, `dbt-superset-lineage`
        # will try to automatically convert snake_cased column names
        # to Title Cased names. Here: `Column 1`.
        verbose_name: My Column 1

        # If a `unit` is provided, it will be automatically appended
        # to the `verbose_name` and enclosed in brackets.
        # Here: `My Column 1 [min]`.
        unit: min

        # More BI-specific settings are placed in the `bi_integration` node.
        # We may use YAML anchors for re-using previously defined settings.
        # In this example the anchor is called `bi_enable_all`:
        bi_integration: &bi_enable_all
          # Whether this column is to be exposed in filter configuration dialogs.
          # If not specified, this property defaults to `true`.
          is_filterable: true
          # Whether this column is usable for grouping by it.
          # If not specified, this property defaults to `true`.
          is_groupable: true

    - name: column_2
      description: Another column description.
      meta:
        verbose_name: My 2nd column
        # Referring to the YAML anchor above:
        bi_integration: *bi_enable_all

```


## License

Licensed under the MIT license (see [LICENSE.md](LICENSE.md) file for more details).


