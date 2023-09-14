# KP-DataProcessing

## Description

This package provides the functionality required by very small company called **KommatiPara** that deals with bitcoin
trading. The basic aim of processing steps is to join information from two separate datasets: one containing clients
data and the other one containing their financial details. Additionally, personal data is excluded  (except from
clients\' e-mails), output is filtered as per chosen criteria and columns are renamed to make the output more
user-friendly.

## Input parameters

- -cds / --customers_data_path - path to dataset with customer data
  (mandatory, no default value)
- -fds / --finance_data_path - path to dataset with finance data
  (mandatory, no default value)
- -rdict / --rename_dict - json string defining mapping between the column to be filtered and values to be used for
  filtering\
  (optional, default value: '{\"id\": \"client_identifier\", \"btc_a\": \"bitcoin_address\", \"cc_t\":
  \"credit_card_type\"}')
- -fdict / --filter_dict - json string defining mapping between column and values to be considered for filtering\
  (optional, default value: '{\"country\": [\"Netherlands\", \"United Kingdom\"]}')

## Input usage hints

- Customer and finance datasets must contain original columns as specified below:<br/><br/>
  **customer_data**
  | id | first_name | last_name | email | country |
  |----|------------|-----------|-------|---------|

  **finance_data**
  | id | btc_a | cc_t | cc_n |
  |----|-------|------|------|

  If that condition is satisfied, application can handle the case in which datasets contain additional columns.
- Json string used for columns renaming step (-rdict) can include columns from both datasets as it\'s applied to already
  joined data.
- Json string used for filtering (-fdict) can include filter criteria for any column from any dataset as it\'s also
  applied to already joined data.

## Output

The output csv file is saved in client_data directory, in folder named output<execution_timesamp>.\
Output file contains joined customer and finance data, without some PII data (first_name, last_name,
cc_n [i.e. credit card number]).\
The data is filtered as specified by -fdict parameter and columns are named as specified by -rdict parameter.\
In default case, output contains only data for customers from Netherlands and United Kingdom and the output columns are
named as per below:
| client_identifier | email | country | bitcoin_address | credit_card_type |
|-------------------|-------|---------|-----------------|------------------|
--------------------------------------------------------

## Examples:

### Usecase 1.

Use default settings:

```
python -m src.main -cds "./datasets/dataset_one.csv" -fds "./datasets/dataset_two.csv"
```

### Usecase 2.

Use options required in task description (accept 3 parameters: two datasets paths and country filter):

```
python -m src.main -cds "./datasets/dataset_one.csv" -fds "./datasets/dataset_two.csv" -fdict "{\"country\": [\"France\", \"United Kingdom\"]}"
```

### Usecase 3.

Complex usage, employing all customizable options:

```
python -m src.main -cds "./datasets/dataset_one.csv" -fds "./datasets/dataset_two.csv" -fdict "{\"country\": [\"France\", \"United States\"], \"cc_t\": [\"visa\", \"visa-electron\", \"mastercard\"]}" -rdict "{\"cc_t\": \"type of credit card\", \"country\": \"customer's country\", \"email\": \"customer's contact email\"}"
```
