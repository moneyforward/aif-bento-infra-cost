# aif-bento-infra-cost
Cost Identification on Bento Infra 

## How to use
- Install packages:
    - Create venv, run this:
        ```
        python -m venv venv
        ```
    - Activate the virtual environment:
        ```
        source venv/bin/activate
        ```
    - Install packages:
        ```
        pip install pandas boto3 matplotlib seaborn flask
        ```
- Activate the virtual environment if you're not activate it, run this:
    ```
    source venv/bin/activate
    ```
- Download `costs.csv` files in `prod-bento`
    - Open accounts aws
    - Search for Billing and Cost Management Home
    - Go to Cost Explorer Tab
    - Pick your Date Range (Monthly)
    - Click Download as CSV
    - You will get costs.csv
- Replace `costs.csv` in our folder with new `costs.csv` that we downloaded
- Change this section inside `read_costs.py` with your needed:
    ```
    start_date = "YYYY-MM-DD"
    end_date = "YYYY-MM-DD"
    aws_account_id = "AWS_ACCOUNT_ID"
    region = 'YOUR_REGION'
    ```
- Export you AWS Credentials from AWS.
- Run python scripts to get Cost identification csv files.
    ```
    python read_costs.py
    ```
- After run the scripts, you will get a lot of csv. You can open the `all_costs.csv` to get the all details to filled in sheets.

