import pandas as pd
import boto3
import json
import matplotlib.pyplot as plt
import seaborn as sns

start_date = "2025-02-01"
end_date = "2025-02-24"
aws_account_id = "093411005699"

#download the costs.csv file from cost explorer for specific month
def get_service_names(csv_file="costs.csv"):
    df = pd.read_csv(csv_file)
    service_columns = [col.replace('($)', '') for col in df.columns 
                      if col.endswith('($)') and col not in ['Service($)', 'Total costs($)']]
    return service_columns

#read the costs.csv file from cost explorer for specific month
#example: read_costs("CloudWatch", "costs.csv")
def read_costs(service="CloudWatch", csv_file="costs.csv", return_value=False):
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Get service costs
    service_column = f"{service}($)"
    service_costs = df[service_column].iloc[0]
    if not return_value:
        print(f"{service} costs: ${service_costs}")
    return service_costs


# Json file is the resources.json file in the bento-platform-model-inference-prod-resources folder
# Example 2: Read all CloudWatch log groups
# cloudwatch_logs = read_json_file(json_path="model-inference.monitoring.cloudwatch.resources")

def read_json_file(json_file="resources.json", json_path=None, return_value=False):
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            
            if json_path:
                # Split path into parts
                path_parts = json_path.split('.')
                result = data
                
                # Navigate through the path
                for part in path_parts:
                    if part.isdigit():  # Handle array indices
                        result = result[int(part)]
                    else:
                        result = result[part]
                
                if not return_value:
                    print(f"\nData at path '{json_path}':")
                    print(json.dumps(result, indent=2))
                return result
            else:
                if not return_value:
                    print(f"\nContents of {json_file}:")
                    print(json.dumps(data, indent=2))
                return data
                
    except FileNotFoundError:
        print(f"Error: File '{json_file}' not found")
        return None
    except (KeyError, IndexError):
        print(f"Error: Path '{json_path}' not found in JSON")
        return None
    except json.JSONDecodeError:
        print(f"Error: '{json_file}' is not a valid JSON file")
        return None
    except Exception as e:
        print(f"Error reading JSON file: {str(e)}")
        return None

# Get the cloudwatch log groups from the cloudwatch logs client
def get_cloudwatch_log_groups():
    try:
        # Initialize CloudWatch Logs client
        logs_client = boto3.client('logs')
        
        # Get all log groups
        log_groups = []
        total_size_gb = 0
        paginator = logs_client.get_paginator('describe_log_groups')
        
        for page in paginator.paginate():
            for log_group in page['logGroups']:
                name = log_group['logGroupName']
                size = log_group.get('storedBytes', 0)
                size_gb = size / (1024 * 1024 * 1024)  # Convert to GB
                total_size_gb += size_gb
                log_groups.append({
                    'name': name,
                    'size_gb': round(size_gb, 2)
                })
        
        # Sort by size
        log_groups.sort(key=lambda x: x['size_gb'], reverse=True)
        
        # Print results
        for group in log_groups:
            print(f"Log Group: {group['name']}")
            print(f"Size: {group['size_gb']} GB\n")
            
        print(f"Total size of all log groups: {round(total_size_gb, 2)} GB")
        return log_groups
        
    except Exception as e:
        print(f"Error fetching CloudWatch logs: {str(e)}")
        return []

# Get the cost per GB for the cloudwatch logs
def get_cloudwatch_cost_per_gb():
    try:
        # Get total CloudWatch costs using read_costs function
        cloudwatch_costs = read_costs("CloudWatch", return_value=True)
        
        # Get total log size
        log_groups = get_cloudwatch_log_groups()
        total_size_gb = sum(group['size_gb'] for group in log_groups)
        
        # Calculate cost per GB
        cost_per_gb = cloudwatch_costs / total_size_gb if total_size_gb > 0 else 0
        
        print(f"\nCloudWatch Analysis:")
        print(f"Total Cost: ${cloudwatch_costs}")
        print(f"Total Size: {round(total_size_gb, 2)} GB")
        print(f"Cost per GB: ${round(cost_per_gb, 2)}")
        
        return cost_per_gb
        
    except Exception as e:
        print(f"Error calculating cost per GB: {str(e)}")
        return 0

# Get the cost for a specific log group which is in the resources.json file
def get_specific_log_group_cost(log_group_name=None, output_file="log_group_costs.csv", return_value=False):
    try:
        if log_group_name is None:
            cloudwatch_logs = read_json_file(json_path="model-inference.monitoring.cloudwatch.resources", return_value=True)
            if not cloudwatch_logs:
                return None
        else:
            cloudwatch_logs = [log_group_name]
            
        # Get the cost per GB for the cloudwatch logs
        cost_per_gb = get_cloudwatch_cost_per_gb()
        # Get the cloudwatch log groups from the cloudwatch logs client
        all_log_groups = get_cloudwatch_log_groups()
        
        results = []
        for log_name in cloudwatch_logs:
            matching_group = next((group for group in all_log_groups if group['name'] == log_name), None)
            if matching_group:
                size_gb = matching_group['size_gb']
                cost = size_gb * cost_per_gb
                results.append({
                    'service': 'CloudWatch',
                    'log_group_name': log_name,
                    'size_gb': size_gb,
                    'cost_usd': round(cost, 2),
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'cost_per_gb': round(cost_per_gb, 2)
                })
                print(f"\nLog Group: {log_name}")
                print(f"Size: {size_gb} GB")
                print(f"Cost per GB: ${round(cost_per_gb, 2)}")
                print(f"Total Cost: ${round(cost, 2)}")
                print(f"AWS Account: {aws_account_id}")
                print(f"Period: {start_date} to {end_date}")
        
        if results:
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
                
        return results
        
    except Exception as e:
        print(f"Error calculating specific log group cost: {str(e)}")
        return None
    
# Get the cost for the ECR repositories which is in the resources.json file
def get_ecr_repos_cost(output_file="ecr_costs.csv", return_value=False):
    # Create client for ECR
    ecr_client = boto3.client('ecr')
    
    try:
        # Get ECR repositories from resources.json
        json_repos = read_json_file(json_path="model-inference.storage.ecr.resources", return_value=True)
        if not json_repos:
            print("No ECR repositories found in resources.json")
            return None

        repositories_data = []
        total_size_gb = 0
        total_cost = 0
        
        # Process only repositories from resources.json
        for repo_name in json_repos:
            # Add 'prod/' prefix to repository name
            full_repo_name = f"prod/{repo_name}"
            repo_size_bytes = 0
            
            try:
                images_paginator = ecr_client.get_paginator('describe_images')
                for images_page in images_paginator.paginate(repositoryName=full_repo_name):
                    for image in images_page['imageDetails']:
                        repo_size_bytes += image.get('imageSizeInBytes', 0)
                        
                repo_size_gb = repo_size_bytes / (1024 * 1024 * 1024)
                total_size_gb += repo_size_gb
                repo_cost = repo_size_gb * 0.10  # $0.10 per GB
                total_cost += repo_cost
                
                repositories_data.append({
                    'service': 'ECR',
                    'repository_name': full_repo_name,
                    'size_gb': round(repo_size_gb, 2),
                    'cost_usd': round(repo_cost, 2),
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date
                })
                
            except Exception as e:
                print(f"Warning: Could not get images for repository {full_repo_name}: {str(e)}")
                continue

        if repositories_data:
            # Save to CSV using pandas
            df = pd.DataFrame(repositories_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nECR Repository Costs:")
            print("="*50)
            for repo in sorted(repositories_data, key=lambda x: x['size_gb'], reverse=True):
                print(f"\nRepository: {repo['repository_name']}")
                print(f"Size: {repo['size_gb']} GB")
                print(f"Cost: ${repo['cost_usd']}")
                print("-"*30)
            
            print(f"\nTotal ECR Storage: {round(total_size_gb, 2)} GB")
            print(f"Total ECR Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return repositories_data

    except Exception as e:
        print(f"Error calculating ECR costs: {str(e)}")
        return None

def get_ec2_instance_costs(output_file="ec2_costs.csv", return_value=False):
    ce_client = boto3.client('ce')
    
    try:
        # Get total EC2 cost from CSV
        total_ec2_cost = read_costs("EC2-Instances", return_value=True)
        
        # Get EC2 costs filtered by instance types
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
                {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}
            ],
            Filter={
                'And': [
                    {
                        'Dimensions': {
                            'Key': 'SERVICE',
                            'Values': ['Amazon Elastic Compute Cloud - Compute']
                        }
                    },
                    {
                        'Dimensions': {
                            'Key': 'INSTANCE_TYPE',
                            'Values': ['t3.large', 'g4dn.xlarge']
                        }
                    }
                ]
            }
        )

        instance_data = []
        for group in response['ResultsByTime'][0]['Groups']:
            instance_type, usage_type = group['Keys']
            cost = float(group['Metrics']['UnblendedCost']['Amount'])
            
            instance_data.append({
                'service': 'EC2',
                'instance_type': instance_type,
                'usage_type': usage_type,
                'cost_usd': round(cost, 2),
                'account_id': aws_account_id,
                'start_date': start_date,
                'end_date': end_date
            })

        if instance_data:
            df = pd.DataFrame(instance_data)
            df = df.sort_values(['instance_type', 'cost_usd'], ascending=[True, False])
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nCosts by Instance Type:")
            instance_costs = df.groupby('instance_type')['cost_usd'].sum()
            for instance_type, cost in instance_costs.items():
                print(f"\n{instance_type} (Tokyo Region):")
                print(f"Total Cost: ${round(cost, 2)}")
                usage_costs = df[df['instance_type'] == instance_type]
                for _, row in usage_costs.iterrows():
                    usage_desc = "Running hours" if "BoxUsage" in row['usage_type'] else row['usage_type']
                    print(f"  {usage_desc}: ${row['cost_usd']}")
            
            print(f"\nFiltered EC2 Instance Cost: ${round(df['cost_usd'].sum(), 2)}")
            print(f"Total EC2 Instance Cost: ${total_ec2_cost}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return instance_data

    except Exception as e:
        print(f"Error calculating EC2 costs: {str(e)}")
        return None
    

def get_aurora_storage_costs(output_file="aurora_storage_costs.csv", return_value=False):
    ce_client = boto3.client('ce')
    
    try:
        # Get RDS Aurora storage-related costs
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost', 'UsageQuantity'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}
            ],
            Filter={
                'And': [
                    {
                        'Dimensions': {
                            'Key': 'SERVICE',
                            'Values': ['Amazon Relational Database Service']
                        }
                    },
                    {
                        'Dimensions': {
                            'Key': 'USAGE_TYPE',
                            'Values': [
                                'APN1-Aurora:StorageUsage',
                                'APN1-Aurora:StorageIOUsage',
                                'APN1-Aurora:BackupUsage'
                            ]
                        }
                    }
                ]
            }
        )

        storage_data = []
        for group in response['ResultsByTime'][0]['Groups']:
            usage_type = group['Keys'][0]
            cost = float(group['Metrics']['UnblendedCost']['Amount'])
            usage = float(group['Metrics']['UsageQuantity']['Amount'])
            
            # Map usage types to readable names
            usage_name = {
                'APN1-Aurora:StorageUsage': 'Storage',
                'APN1-Aurora:StorageIOUsage': 'I/O Operations',
                'APN1-Aurora:BackupUsage': 'Backup Storage'
            }.get(usage_type, usage_type)
            
            # Map units based on usage type
            unit = 'GB-Month' if 'Storage' in usage_type else 'IOs' if 'IO' in usage_type else ''
            
            storage_data.append({
                'service': 'RDS',
                'usage_type': usage_type,
                'usage_name': usage_name,
                'usage_quantity': round(usage, 2),
                'unit': unit,
                'cost_usd': round(cost, 2),
                'account_id': aws_account_id,
                'start_date': start_date,
                'end_date': end_date
            })

        if storage_data:
            df = pd.DataFrame(storage_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nAurora Storage Costs Breakdown:")
            for data in storage_data:
                print(f"\n{data['usage_name']}:")
                print(f"Usage: {data['usage_quantity']} {data['unit']}")
                print(f"Cost: ${data['cost_usd']}")
            
            total_cost = sum(data['cost_usd'] for data in storage_data)
            print(f"\nTotal Aurora Storage Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return storage_data

    except Exception as e:
        print(f"Error calculating Aurora storage costs: {str(e)}")
        return None

def get_eks_costs(output_file="eks_costs.csv", return_value=False):
    try:
        # Get EKS cost from CSV
        total_eks_cost = read_costs("Elastic Container Service for Kubernetes", return_value=True)
        divided_eks_cost = total_eks_cost / 2  # Dividing cost between two clusters
        
        # Get cluster names from resources.json
        eks_cluster_names = read_json_file(json_path="model-inference.compute.eks.resources", return_value=True)
        if not eks_cluster_names:
            print("No EKS clusters found in resources.json")
            return None

        eks_data = []
        
        # Create data for each cluster
        for cluster_name in eks_cluster_names:
            eks_data.append({
                'service': 'EKS',
                'cluster_name': cluster_name,
                'original_cost_usd': round(total_eks_cost, 2),
                'cost_usd': round(divided_eks_cost, 2),
                'account_id': aws_account_id,
                'start_date': start_date,
                'end_date': end_date
            })
        
        if eks_data:
            # Save to CSV
            df = pd.DataFrame(eks_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nEKS Cluster Costs:")
            print("="*50)
            for cluster in eks_data:
                print(f"\nCluster: {cluster['cluster_name']}")
                print(f"Original Cost: ${cluster['original_cost_usd']}")
                print(f"Divided Cost: ${cluster['cost_usd']}")
                print("-"*30)
            
            print(f"\nTotal Clusters: {len(eks_data)}")
            print(f"Total Original Cost: ${round(total_eks_cost, 2)}")
            print(f"Total Divided Cost: ${round(total_eks_cost/2, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return eks_data

    except Exception as e:
        print(f"Error calculating EKS costs: {str(e)}")
        return None

def get_dynamodb_costs(output_file="dynamodb_costs.csv", return_value=False):
    dynamodb = boto3.client('dynamodb')
    
    try:
        # Get DynamoDB tables from resources.json
        json_tables = read_json_file(json_path="model-inference.databases.dynamodb.resources", return_value=True)
        if not json_tables:
            print("No DynamoDB tables found in resources.json")
            return None

        table_data = []
        total_size_gb = 0
        
        for table_arn in json_tables:
            table_name = table_arn.split('table/')[-1]
            
            try:
                # Get table size
                response = dynamodb.describe_table(TableName=table_name)
                size_bytes = response['Table'].get('TableSizeBytes', 0)
                size_gb = size_bytes / (1024 * 1024 * 1024)  # Convert to GB
                total_size_gb += size_gb
                
                table_data.append({
                    'service': 'DynamoDB',
                    'table_name': table_name,
                    'table_arn': table_arn,
                    'size_gb': round(size_gb, 2),
                    'cost_usd': round(size_gb * 0.285, 2),
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date
                })
                
            except Exception as e:
                print(f"Warning: Could not get details for table {table_name}: {str(e)}")
                continue

        if table_data:
            # Calculate costs
            billable_gb = max(0, total_size_gb)  
            total_cost = billable_gb * 0.285  # $0.285 per GB-Month
            
            # Add cost to each table proportionally
            for table in table_data:
                table['cost_usd'] = round((table['size_gb'] * total_cost) if total_size_gb > 0 else 0, 2)
            
            # Save to CSV
            df = pd.DataFrame(table_data)
            df = df.sort_values('size_gb', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            # Print summary
            print("\nDynamoDB Storage Costs Breakdown:")
            for table in table_data:
                print(f"\nTable: {table['table_name']}")
                print(f"Size: {table['size_gb']} GB")
                print(f"Cost: ${table['cost_usd']}")
            
            print(f"\nTotal Storage: {round(total_size_gb, 2)} GB")
            print(f"Billable Storage (after free tier): {round(billable_gb, 2)} GB")
            print(f"Total Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return table_data

    except Exception as e:
        print(f"Error calculating DynamoDB costs: {str(e)}")
        return None

def get_apigateway_costs(output_file="apigateway_costs.csv", return_value=False):
    try:
        # Get API Gateway cost from CSV
        total_api_cost = read_costs("API Gateway", return_value=True)
        
        # Create DataFrame with costs
        api_data = [{
            'service': 'API Gateway',
            'cost_usd': round(total_api_cost, 2),
            'account_id': aws_account_id,
            'start_date': start_date,
            'end_date': end_date
        }]
        
        # Save to CSV
        df = pd.DataFrame(api_data)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        print("\nAPI Gateway Costs:")
        print(f"Total Cost: ${round(total_api_cost, 2)}")
        print(f"AWS Account: {aws_account_id}")
        print(f"Period: {start_date} to {end_date}")

        return api_data

    except Exception as e:
        print(f"Error calculating API Gateway costs: {str(e)}")
        return None

def get_waf_costs(output_file="waf_costs.csv", return_value=False):
    try:
        # Get WAF cost from CSV
        total_waf_cost = read_costs("WAF", return_value=True)
        
        # Create DataFrame with costs
        waf_data = [{
            'service': 'WAF',
            'cost_usd': round(total_waf_cost, 2),
            'account_id': aws_account_id,
            'start_date': start_date,
            'end_date': end_date
        }]
        
        # Save to CSV
        df = pd.DataFrame(waf_data)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        print("\nWAF Costs:")
        print(f"Total Cost: ${round(total_waf_cost, 2)}")
        print(f"AWS Account: {aws_account_id}")
        print(f"Period: {start_date} to {end_date}")

        return waf_data

    except Exception as e:
        print(f"Error calculating WAF costs: {str(e)}")
        return None

def get_specific_s3_costs(return_value=False):
    s3_client = boto3.client('s3')
    
    try:
        # Get S3 buckets from resources.json
        json_buckets = read_json_file(json_path="model-inference.storage.s3.resources", return_value=True)
        if not json_buckets:
            print("No S3 buckets found in resources.json")
            return None

        bucket_data = []
        total_cost = 0

        for bucket_arn in json_buckets:
            # Extract bucket name from ARN
            bucket_name = bucket_arn.split(':')[-1]
            
            try:
                # Initialize counters
                total_size_bytes = 0
                total_objects = 0

                # Get all objects in the bucket recursively
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket_name):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            total_size_bytes += obj['Size']
                            total_objects += 1

                # Convert to GB
                total_size_gb = total_size_bytes / (1024 * 1024 * 1024)
                
                # Calculate storage cost ($0.023 per GB per month for S3 Standard)
                storage_cost = total_size_gb * 0.023

                bucket_data.append({
                    'service': 'S3',
                    'name': bucket_name,
                    'arn': bucket_arn,
                    'storage_gb': total_size_gb,
                    'storage_cost': storage_cost,
                    'total_objects': total_objects,
                    'cost_usd': storage_cost,
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date
                })

                total_cost += storage_cost

            except Exception as e:
                print(f"Warning: Could not process bucket {bucket_name}: {str(e)}")
                continue

        if bucket_data:
            # Create CSV report
            df = pd.DataFrame(bucket_data)
            df = df.sort_values('storage_gb', ascending=False)
            df.to_csv('s3_costs.csv', index=False)
            print(f"\nResults saved to s3_costs.csv")

            print(f"\nS3 Bucket Size Summary:")
            print("="*50)
            for bucket in sorted(bucket_data, key=lambda x: x['storage_gb'], reverse=True):
                print(f"Bucket: {bucket['name']}")
                print(f"Total Objects: {bucket['total_objects']:,}")
                print(f"Storage: {bucket['storage_gb']:.2f} GB")
                print(f"Storage Cost: ${bucket['storage_cost']:.2f}")
                print("-"*50)
            
            total_gb = sum(b['storage_gb'] for b in bucket_data)
            print(f"\nTotal Objects: {sum(b['total_objects'] for b in bucket_data):,}")
            print(f"Total Storage: {total_gb:.2f} GB")
            print(f"Total Cost: ${total_cost:.2f}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return bucket_data

    except Exception as e:
        print(f"Error calculating S3 costs: {str(e)}")
        return None

def get_specific_kms_costs(output_file="kms_costs.csv", return_value=False):
    kms_client = boto3.client('kms')
    cloudwatch = boto3.client('cloudwatch')
    
    # KMS pricing for Asia Pacific
    ASIA_PACIFIC_KEY_COST = 1.0  # $1.00 per KMS key per month
    REQUEST_COST = 0.03 / 10000  # $0.03 per 10,000 requests
    
    try:
        # Get KMS keys from resources.json
        json_keys = read_json_file(json_path="model-inference.security.kms.resources", return_value=True)
        if not json_keys:
            print("No KMS keys found in resources.json")
            return None

        key_data = []
        total_cost = 0

        for key_arn in json_keys:
            try:
                # Get key details
                key_id = key_arn.split('key/')[-1]
                
                # Get request count from CloudWatch
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/KMS',
                    MetricName='RequestCount',
                    Dimensions=[{'Name': 'KeyId', 'Value': key_id}],
                    StartTime=start_date,
                    EndTime=end_date,
                    Period=2592000,  # 30 days
                    Statistics=['Sum']
                )
                
                request_count = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
                request_cost = (request_count / 10000) * 0.03  # $0.03 per 10,000 requests
                total_key_cost = ASIA_PACIFIC_KEY_COST + request_cost
                total_cost += total_key_cost

                key_data.append({
                    'service': 'KMS',
                    'key_id': key_id,
                    'key_arn': key_arn,
                    'request_count': int(request_count),
                    'request_cost': round(request_cost, 2),
                    'key_cost': ASIA_PACIFIC_KEY_COST,
                    'cost_usd': round(total_key_cost, 2),
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date
                })

            except Exception as e:
                print(f"Warning: Could not process KMS key {key_id}: {str(e)}")
                continue

        if key_data:
            # Save to CSV
            df = pd.DataFrame(key_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nKMS Key Costs:")
            print("="*50)
            for key in key_data:
                print(f"\nKey ID: {key['key_id']}")
                print(f"Requests: {key['request_count']:,}")
                print(f"Request Cost: ${key['request_cost']}")
                print(f"Key Cost: ${key['key_cost']}")
                print(f"Total Cost: ${key['cost_usd']}")
                print("-"*50)
            
            print(f"\nTotal Keys: {len(key_data)}")
            print(f"Total Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return key_data

    except Exception as e:
        print(f"Error calculating KMS costs: {str(e)}")
        return None

def get_route53_costs(output_file="route53_costs.csv", return_value=False):
    try:
        # Get Route 53 cost from CSV
        total_route53_cost = read_costs("Route 53", return_value=True)
        
        # Create DataFrame with costs
        route53_data = [{
            'service': 'Route 53',
            'cost_usd': round(total_route53_cost, 2),
            'account_id': aws_account_id,
            'start_date': start_date,
            'end_date': end_date
        }]
        
        # Save to CSV
        df = pd.DataFrame(route53_data)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        print("\nRoute 53 Costs:")
        print(f"Total Cost: ${round(total_route53_cost, 2)}")
        print(f"AWS Account: {aws_account_id}")
        print(f"Period: {start_date} to {end_date}")

        return route53_data

    except Exception as e:
        print(f"Error calculating Route 53 costs: {str(e)}")
        return None

def get_sqs_costs(output_file="sqs_costs.csv", return_value=False):
    try:
        # Get SQS cost from CSV
        total_sqs_cost = read_costs("SQS", return_value=True)
        
        # Create DataFrame with costs
        sqs_data = [{
            'service': 'SQS',
            'cost_usd': round(total_sqs_cost, 2),
            'account_id': aws_account_id,
            'start_date': start_date,
            'end_date': end_date
        }]
        
        # Save to CSV
        df = pd.DataFrame(sqs_data)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        print("\nSQS Costs:")
        print(f"Total Cost: ${round(total_sqs_cost, 2)}")
        print(f"AWS Account: {aws_account_id}")
        print(f"Period: {start_date} to {end_date}")

        return sqs_data

    except Exception as e:
        print(f"Error calculating SQS costs: {str(e)}")
        return None

def get_vpc_costs(output_file="vpc_costs.csv", return_value=False):
    ec2_client = boto3.client('ec2')
    cloudwatch = boto3.client('cloudwatch')
    
    try:
        # Get VPC info from resources.json
        json_vpcs = read_json_file(json_path="model-inference.networking.vpc.resources", return_value=True)
        if not json_vpcs:
            print("No VPCs found in resources.json")
            return None

        vpc_data = []
        total_cost = 0  # Initialize total cost

        for vpc_arn in json_vpcs:
            vpc_id = vpc_arn.split('vpc/')[-1]
            vpc_cost = {
                'vpc_id': vpc_id,
                'vpn_connection_hours': 0,
                'data_transfer_out': 0
            }

            # Get all instances in the VPC
            instances = ec2_client.describe_instances(
                Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
            )['Reservations']

            # Calculate data transfer for instances
            total_bytes_out = 0
            for reservation in instances:
                for instance in reservation['Instances']:
                    if instance['State']['Name'] == 'running':
                        instance_id = instance['InstanceId']
                        try:
                            response = cloudwatch.get_metric_statistics(
                                Namespace='AWS/EC2',
                                MetricName='NetworkOut',
                                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                                StartTime=start_date,
                                EndTime=end_date,
                                Period=2592000,
                                Statistics=['Sum']
                            )
                            if response['Datapoints']:
                                total_bytes_out += response['Datapoints'][0]['Sum']
                        except Exception as e:
                            print(f"Warning: Could not get metrics for Instance {instance_id}: {str(e)}")

            vpc_cost['data_transfer_out'] = total_bytes_out

            # Get VPN Connections
            vpn_connections = ec2_client.describe_vpn_connections(
                Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
            )['VpnConnections']

            active_vpn_count = sum(1 for vpn in vpn_connections if vpn['State'] != 'deleted')
            vpc_cost['vpn_connection_hours'] = active_vpn_count * 24 * 30

            # Calculate costs
            vpn_connection_cost = vpc_cost['vpn_connection_hours'] * 0.005  # $0.005 per hour
            data_transfer_cost = (vpc_cost['data_transfer_out'] / (1024 * 1024 * 1024)) * 0.09  # $0.09 per GB

            vpc_total_cost = vpn_connection_cost + data_transfer_cost
            total_cost += vpc_total_cost  # Add to total cost

            vpc_data.append({
                'service': 'VPC',
                'vpc_id': vpc_id,
                'vpc_arn': vpc_arn,
                'vpn_connections': active_vpn_count,
                'data_transfer_gb': round(vpc_cost['data_transfer_out'] / (1024 * 1024 * 1024), 2),
                'data_transfer_cost': round(data_transfer_cost, 2),
                'vpn_cost': round(vpn_connection_cost, 2),
                'cost_usd': round(vpc_total_cost, 2),
                'account_id': aws_account_id,
                'start_date': start_date,
                'end_date': end_date
            })

        if vpc_data:
            # Save to CSV
            df = pd.DataFrame(vpc_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            
            print("\nVPC Costs Breakdown:")
            print("="*50)
            for vpc in vpc_data:
                print(f"\nVPC: {vpc['vpc_id']}")
                print(f"Data Transfer Out: {vpc['data_transfer_gb']} GB")
                print(f"  - Cost: ${vpc['data_transfer_cost']}")
                print(f"VPN Connections: {vpc['vpn_connections']} (${vpc['vpn_cost']})")
                print(f"Total Cost: ${vpc['cost_usd']}")
                print("-"*50)
            
            print(f"\nTotal VPC Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return vpc_data

    except Exception as e:
        print(f"Error calculating VPC costs: {str(e)}")
        return None
    

def get_nlb_costs(output_file="nlb_costs.csv", return_value=False):
    elb_client = boto3.client('elbv2')
    cloudwatch = boto3.client('cloudwatch')
    
    # Constants for pricing
    NLB_HOURLY_COST = 0.0225  # $0.0225 per hour
    NLB_LCU_COST = 0.006      # $0.006 per LCU-hour
    
    try:
        # Get all NLBs
        nlb_data = []
        cost_usd = 0

        # Get all load balancers
        nlbs = elb_client.describe_load_balancers()['LoadBalancers']
        
        # Filter for Network Load Balancers
        network_lbs = [lb for lb in nlbs if lb['Type'] == 'network']

        for lb in network_lbs:
            try:
                lb_name = lb['LoadBalancerName']
                lb_arn = lb['LoadBalancerArn']
                vpc_id = lb['VpcId']

                # Calculate base cost (hourly cost for the month)
                hours_in_month = 730  # ~30 days
                base_cost = hours_in_month * NLB_HOURLY_COST

                # Get processed bytes for LCU calculation
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/NetworkELB',
                    MetricName='ProcessedBytes',
                    Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_arn.split('/')[-1]}],
                    StartTime=start_date,
                    EndTime=end_date,
                    Period=2592000,  # 30 days in seconds
                    Statistics=['Sum']
                )

                # Calculate LCU cost
                processed_bytes = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
                processed_gb = processed_bytes / (1024 * 1024 * 1024)
                
                # 1 LCU = 1 GB processed
                lcu_hours = processed_gb * hours_in_month
                lcu_cost = lcu_hours * NLB_LCU_COST

                total_lb_cost = base_cost + lcu_cost

                nlb_data.append({
                    'service': 'ELB',
                    'name': lb_name,
                    'arn': lb_arn,
                    'vpc_id': vpc_id,
                    'processed_gb': round(processed_gb, 2),
                    'base_cost': round(base_cost, 2),
                    'lcu_hours': round(lcu_hours, 2),
                    'lcu_cost': round(lcu_cost, 2),
                    'cost_usd': round(total_lb_cost, 2),
                    'account_id': aws_account_id,
                    'start_date': start_date,
                    'end_date': end_date
                })

                cost_usd += total_lb_cost

            except Exception as e:
                print(f"Warning: Could not process NLB {lb_name}: {str(e)}")
                continue

        if nlb_data:
            # Save to CSV
            df = pd.DataFrame(nlb_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")

            print("\nNetwork Load Balancer Costs:")
            print("="*50)
            for lb in nlb_data:
                print(f"\nLoad Balancer: {lb['name']}")
                print(f"VPC: {lb['vpc_id']}")
                print(f"Data Processed: {lb['processed_gb']} GB")
                print(f"Base Cost: ${lb['base_cost']}")
                print(f"LCU Hours: {lb['lcu_hours']}")
                print(f"LCU Cost: ${lb['lcu_cost']}")
                print(f"Total Cost: ${lb['cost_usd']}")
                print("-"*50)
            
            print(f"\nTotal NLBs: {len(nlb_data)}")
            print(f"Total Data Processed: {sum(lb['processed_gb'] for lb in nlb_data):.2f} GB")
            print(f"Total Cost: ${round(cost_usd, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return nlb_data

    except Exception as e:
        print(f"Error calculating NLB costs: {str(e)}")
        return None


def get_nat_gateway_costs(output_file="nat_gateway_costs.csv", return_value=False):
    ec2_client = boto3.client('ec2')
    cloudwatch = boto3.client('cloudwatch')
    
    # NAT Gateway pricing for Asia Pacific
    NAT_GATEWAY_HOURLY = 0.062     # $0.062 per hour
    NAT_GATEWAY_DATA = 0.062       # $0.062 per GB
    
    try:
        nat_data = []
        instance_data = []
        total_nat_running_cost = 0
        total_data_processing_cost = 0

        # Get NAT Gateways with specific tag
        nat_gateways = ec2_client.describe_nat_gateways(
            Filters=[{'Name': 'tag:Name', 'Values': ['vpc-bento-platform-ap-northeast-1a']}]
        )['NatGateways']

        # Get VPC ID from NAT Gateway
        vpc_id = nat_gateways[0]['VpcId'] if nat_gateways else None

        if vpc_id:
            # Get EC2 instances in the same VPC
            instances = ec2_client.describe_instances(
                Filters=[
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )

            # Process EC2 instance data
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    instance_name = next((tag['Value'] for tag in instance.get('Tags', []) 
                                       if tag['Key'] == 'Name'), 'Unknown')

                    try:
                        response = cloudwatch.get_metric_statistics(
                            Namespace='AWS/EC2',
                            MetricName='NetworkOut',
                            Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                            StartTime=start_date,
                            EndTime=end_date,
                            Period=2678400,  # 31 days
                            Statistics=['Sum']
                        )
                        
                        bytes_out = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
                        data_gb = bytes_out / (1024 * 1024 * 1024)
                        
                        instance_data.append({
                            'instance_id': instance_id,
                            'instance_name': instance_name,
                            'data_gb': round(data_gb, 2),
                            'vpc_id': vpc_id,
                            'account_id': aws_account_id,
                            'start_date': start_date,
                            'end_date': end_date
                        })
                        
                    except Exception as e:
                        print(f"Warning: Could not get metrics for Instance {instance_id}: {str(e)}")

        # Process NAT Gateway data
        for nat in nat_gateways:
            if nat['State'] != 'deleted':
                nat_id = nat['NatGatewayId']
                
                # Calculate instance running cost
                monthly_hours = 24 * 31  # December has 31 days
                instance_cost = monthly_hours * NAT_GATEWAY_HOURLY
                
                try:
                    response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/NATGateway',
                        MetricName='BytesProcessed',
                        Dimensions=[{'Name': 'NatGatewayId', 'Value': nat_id}],
                        StartTime=start_date,
                        EndTime=end_date,
                        Period=2678400,
                        Statistics=['Sum']
                    )
                    
                    bytes_processed = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
                    data_gb = bytes_processed / (1024 * 1024 * 1024)
                    data_cost = data_gb * NAT_GATEWAY_DATA
                    
                    nat_data.append({
                        'service': 'EC2-Others-NAT',
                        'nat_id': nat_id,
                        'vpc_id': vpc_id,
                        'instance_cost': round(instance_cost, 2),
                        'data_gb': round(data_gb, 2),
                        'data_cost': round(data_cost, 2),
                        'cost_usd': round(instance_cost + data_cost, 2),
                        'account_id': aws_account_id,
                        'start_date': start_date,
                        'end_date': end_date
                    })
                    
                    total_nat_running_cost += instance_cost
                    total_data_processing_cost += data_cost
                    
                except Exception as e:
                    print(f"Warning: Could not get metrics for NAT Gateway {nat_id}: {str(e)}")

        if nat_data or instance_data:
            # Save NAT Gateway data
            nat_df = pd.DataFrame(nat_data)
            if not nat_df.empty:
                nat_df = nat_df.sort_values('cost_usd', ascending=False)
                nat_df.to_csv(output_file, index=False)
                print(f"\nNAT Gateway results saved to {output_file}")

            # Save instance data
            # if instance_data:
            #     instance_df = pd.DataFrame(instance_data)
            #     instance_df = instance_df.sort_values('data_gb', ascending=False)
            #     instance_df.to_csv('nat_gateway_instance_data.csv', index=False)
            #     print("Instance data saved to nat_gateway_instance_data.csv")

            # Print summary
            print(f"\nNAT Gateway Cost Summary for December 2024:")
            print(f"Period: {start_date} to {end_date}")
            print("="*50)
            
            print("\nNAT Gateway Running Costs:")
            print("-"*30)
            for nat in nat_data:
                print(f"NAT Gateway: {nat['nat_id']}")
                print(f"Running Cost: ${nat['instance_cost']}")
                print(f"Data Processed: {nat['data_gb']} GB")
                print(f"Data Cost: ${nat['data_cost']}")
                print(f"Total Cost: ${nat['cost_usd']}")
                print("-"*30)
            
            total_ec2_data_gb = sum(inst['data_gb'] for inst in instance_data)
            print("\nInstance Data Transfer Summary:")
            print("-"*30)
            print(f"Total EC2 Data Out: {total_ec2_data_gb:.2f} GB")
            print(f"Processing Rate: ${NAT_GATEWAY_DATA}/GB")
            print(f"Data Processing Cost: ${total_data_processing_cost:.2f}")
            
            cost_usd = total_nat_running_cost + total_data_processing_cost
            print(f"\nFinal Cost Summary:")
            print(f"NAT Gateway Running Cost: ${total_nat_running_cost:.2f}")
            print(f"Data Processing Cost: ${total_data_processing_cost:.2f}")
            print(f"Total Cost: ${cost_usd:.2f}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return nat_data

    except Exception as e:
        print(f"Error calculating NAT Gateway costs: {str(e)}")
        return None

def get_eks_ebs_costs(output_file="eks_ebs_costs.csv", return_value=False):
    ec2_client = boto3.client('ec2')
    
    # EBS pricing for Asia Pacific
    GP2_COST_PER_GB = 0.12  # $0.12 per GB-month for gp2
    
    try:
        # Get instances with the specified EKS cluster tag
        instances = ec2_client.describe_instances(
            Filters=[{'Name': 'tag:aws:eks:cluster-name', 'Values': ['bento-platform']}]
        )

        volume_data = []
        total_cost = 0

        # Get volumes attached to EKS instances
        instance_ids = [instance['InstanceId'] 
                       for reservation in instances['Reservations']
                       for instance in reservation['Instances']]

        if instance_ids:
            # Get volumes attached to these instances
            volumes = ec2_client.describe_volumes(
                Filters=[{'Name': 'attachment.instance-id', 'Values': instance_ids}]
            )

            for volume in volumes['Volumes']:
                # Only process gp2 volumes
                if volume['VolumeType'] == 'gp2':
                    size_gb = volume['Size']
                    monthly_cost = size_gb * GP2_COST_PER_GB

                    # Get instance ID and name if attached
                    instance_id = volume['Attachments'][0]['InstanceId'] if volume['Attachments'] else 'Unattached'
                    
                    # Get instance name from tags
                    instance_name = 'Unknown'
                    if instance_id != 'Unattached':
                        instance = ec2_client.describe_instances(InstanceIds=[instance_id])
                        instance_name = next((tag['Value'] 
                                           for tag in instance['Reservations'][0]['Instances'][0].get('Tags', [])
                                           if tag['Key'] == 'Name'), 'Unknown')

                    volume_data.append({
                        'service': 'EC2-Others-EBS',
                        'volume_id': volume['VolumeId'],
                        'instance_id': instance_id,
                        'instance_name': instance_name,
                        'size_gb': size_gb,
                        'cost_usd': round(monthly_cost, 2),
                        'account_id': aws_account_id,
                        'start_date': start_date,
                        'end_date': end_date
                    })

                    total_cost += monthly_cost

        if volume_data:
            # Save to CSV using pandas
            df = pd.DataFrame(volume_data)
            df = df.sort_values('cost_usd', ascending=False)
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")

            print(f"\nEKS EBS Volume Cost Summary (bento-platform):")
            print("="*50)
            for volume in sorted(volume_data, key=lambda x: x['cost_usd'], reverse=True):
                print(f"\nVolume: {volume['volume_id']}")
                print(f"Instance: {volume['instance_name']} ({volume['instance_id']})")
                print(f"Size: {volume['size_gb']} GB")
                print(f"Monthly Cost: ${volume['cost_usd']}")
                print("-"*50)
            
            total_size = sum(v['size_gb'] for v in volume_data)
            print(f"\nTotal Volumes: {len(volume_data)}")
            print(f"Total Storage: {total_size} GB")
            print(f"Total Monthly Cost: ${round(total_cost, 2)}")
            print(f"AWS Account: {aws_account_id}")
            print(f"Period: {start_date} to {end_date}")

        return volume_data

    except Exception as e:
        print(f"Error calculating EKS EBS costs: {str(e)}")
        return None

def combine_all_costs(output_file="all_costs.csv"):
    try:
        all_costs = []
        
        # Get CloudWatch costs
        cloudwatch_costs = get_specific_log_group_cost(return_value=True)
        if cloudwatch_costs:
            all_costs.extend(cloudwatch_costs)
            
        # Get ECR costs
        ecr_costs = get_ecr_repos_cost(return_value=True)
        if ecr_costs:
            all_costs.extend(ecr_costs)
            
        # Get EC2 instance costs
        ec2_costs = get_ec2_instance_costs(return_value=True)
        if ec2_costs:
            all_costs.extend(ec2_costs)
            
        # Get Aurora costs
        aurora_costs = get_aurora_storage_costs(return_value=True)
        if aurora_costs:
            all_costs.extend(aurora_costs)
            
        # Get EKS costs
        eks_costs = get_eks_costs(return_value=True)
        if eks_costs:
            all_costs.extend(eks_costs)
            
        # Get DynamoDB costs
        dynamodb_costs = get_dynamodb_costs(return_value=True)
        if dynamodb_costs:
            all_costs.extend(dynamodb_costs)
            
        # Get API Gateway costs
        apigateway_costs = get_apigateway_costs(return_value=True)
        if apigateway_costs:
            all_costs.extend(apigateway_costs)
            
        # Get WAF costs
        waf_costs = get_waf_costs(return_value=True)
        if waf_costs:
            all_costs.extend(waf_costs)
            
        # Get S3 costs
        s3_costs = get_specific_s3_costs(return_value=True)
        if s3_costs:
            all_costs.extend(s3_costs)
            
        # Get KMS costs
        kms_costs = get_specific_kms_costs(return_value=True)
        if kms_costs:
            all_costs.extend(kms_costs)
            
        # Get Route53 costs
        route53_costs = get_route53_costs(return_value=True)
        if route53_costs:
            all_costs.extend(route53_costs)
            
        # Get SQS costs
        sqs_costs = get_sqs_costs(return_value=True)
        if sqs_costs:
            all_costs.extend(sqs_costs)
            
        # Get VPC costs
        vpc_costs = get_vpc_costs(return_value=True)
        if vpc_costs:
            all_costs.extend(vpc_costs)
            
        # Get NLB costs
        nlb_costs = get_nlb_costs(return_value=True)
        if nlb_costs:
            all_costs.extend(nlb_costs)
            
        # Get NAT Gateway costs
        nat_costs = get_nat_gateway_costs(return_value=True)
        if nat_costs:
            all_costs.extend(nat_costs)
            
        # Get EBS costs
        ebs_costs = get_eks_ebs_costs(return_value=True)
        if ebs_costs:
            all_costs.extend(ebs_costs)

        if all_costs:
            # Create DataFrame and save to CSV
            df = pd.DataFrame(all_costs)
            df = df.sort_values(['start_date', 'end_date', 'account_id', 'service', 'cost_usd'], ascending=[True, True, True, True, False])
            df.to_csv(output_file, index=False)
            print(f"\nAll costs saved to {output_file}")
            
            # Print summary by service
            print("\nCost Summary by Service:")
            print("="*50)
            service_summary = df.groupby('service')['cost_usd'].sum()
            for service, cost in service_summary.items():
                print(f"{service}: ${round(cost, 2)}")
            print("-"*50)
            print(f"Total Cost: ${round(service_summary.sum(), 2)}")
            
        return all_costs

    except Exception as e:
        print(f"Error combining costs: {str(e)}")
        return None

def visualize_costs(csv_file="all_costs.csv"):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Create figure with subplots
        plt.style.use('ggplot')
        fig = plt.figure(figsize=(15, 10))
        
        # 1. Service Cost Distribution (Pie Chart)
        plt.subplot(2, 2, 1)
        service_costs = df.groupby('service')['cost_usd'].sum()
        plt.pie(service_costs.values, labels=service_costs.index, autopct='%1.1f%%')
        plt.title('Cost Distribution by Service')
        
        # 2. Service Costs (Bar Chart)
        plt.subplot(2, 2, 2)
        service_costs.plot(kind='bar')
        plt.title('Total Cost by Service')
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Cost (USD)')
        
        # 3. Daily Cost Trend
        plt.subplot(2, 2, 3)
        df['start_date'] = pd.to_datetime(df['start_date'])
        daily_costs = df.groupby('start_date')['cost_usd'].sum()
        daily_costs.plot(kind='line', marker='o')
        plt.title('Daily Cost Trend')
        plt.xlabel('Date')
        plt.ylabel('Cost (USD)')
        plt.xticks(rotation=45)
        
        # 4. Top 10 Most Expensive Resources
        plt.subplot(2, 2, 4)
        resource_cols = ['log_group_name', 'repository_name', 'instance_type', 'cluster_name', 
                        'table_name', 'name', 'key_id', 'vpc_id', 'nat_id', 'volume_id']
        
        # Combine all resource names into a single column
        df['resource_name'] = df[resource_cols].fillna('').agg(' '.join, axis=1).str.strip()
        top_resources = df.nlargest(10, 'cost_usd')
        plt.barh(top_resources['resource_name'], top_resources['cost_usd'])
        plt.title('Top 10 Most Expensive Resources')
        plt.xlabel('Cost (USD)')
        
        # Adjust layout and display
        plt.tight_layout()
        plt.savefig('cost_analysis.png')
        print("\nVisualization saved as cost_analysis.png")
        
        # Additional text summary
        print("\nSummary Statistics:")
        print("="*50)
        print(f"Total Cost: ${df['cost_usd'].sum():.2f}")
        print(f"Average Daily Cost: ${df.groupby('start_date')['cost_usd'].sum().mean():.2f}")
        print(f"Most Expensive Service: {service_costs.idxmax()} (${service_costs.max():.2f})")
        print(f"Most Expensive Day: {daily_costs.idxmax().strftime('%Y-%m-%d')} (${daily_costs.max():.2f})")
        
    except Exception as e:
        print(f"Error creating visualization: {str(e)}")

if __name__ == "__main__":
    combine_all_costs()
    visualize_costs()