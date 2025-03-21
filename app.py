from flask import Flask, render_template, jsonify
import pandas as pd
import os
from read_costs import combine_all_costs

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/costs/<month>')
def get_costs(month):
    # Map month to corresponding file
    file_mapping = {
        'feb-2025': 'all_costs.csv',
        'jan-2025': 'all_costs-jan.csv',
        'dec-2024': 'all_costs-dec.csv'
    }
    
    try:
        # Verify file exists
        filename = file_mapping.get(month)
        if not filename or not os.path.exists(filename):
            print(f"File not found for month: {month}")
            # Generate file if it doesn't exist
            combine_all_costs()
        
        # Get selected month's data
        selected_df = pd.read_csv(filename)
        if selected_df.empty:
            return jsonify({
                'error': f'No data available for {month}',
                'service_costs': {},
                'monthly_trend': {'months': [], 'costs': []},
                'top_resources': [],
                'summary': {
                    'total_cost': 0,
                    'avg_monthly_cost': 0,
                    'most_expensive_service': {'name': '-', 'cost': 0}
                }
            })
        
        # Get all months data for trend
        all_data = []
        for m, file in file_mapping.items():
            try:
                if os.path.exists(file):
                    df = pd.read_csv(file)
                    if not df.empty:
                        df['month'] = m
                        all_data.append(df)
            except Exception as e:
                print(f"Error reading file for {m}: {str(e)}")
                continue
        
        if not all_data:
            print("No data available for trend")
            return jsonify({'error': 'No data available for trend'})
        
        trend_df = pd.concat(all_data)
        
        # Create resource_name column based on service type
        def get_resource_name(row):
            try:
                service_mapping = {
                    'CloudWatch': 'log_group_name',
                    'ECR': 'repository_name',
                    'EC2': 'instance_type',
                    'RDS': 'usage_name',
                    'EKS': 'cluster_name',
                    'DynamoDB': 'table_name',
                    'S3': 'name',
                    'KMS': 'key_id',
                    'VPC': 'vpc_id',
                    'ELB': 'name',
                    'EC2-Others-NAT': 'nat_id',
                    'EC2-Others-EBS': 'volume_id'
                }
                return row.get(service_mapping.get(row['service'], ''), '')
            except:
                return 'Unknown'

        # Apply resource_name to selected month's data
        selected_df['resource_name'] = selected_df.apply(get_resource_name, axis=1)
        
        # Calculate monthly totals for trend
        monthly_costs = trend_df.groupby('month')['cost_usd'].sum().round(2)
        
        # Prepare data for different visualizations using selected month's data
        data = {
            'service_costs': selected_df.groupby('service')['cost_usd'].sum().round(2).to_dict(),
            'monthly_trend': {
                'months': monthly_costs.index.tolist(),
                'costs': monthly_costs.values.tolist()
            },
            'top_resources': selected_df.nlargest(10, 'cost_usd')[['resource_name', 'service', 'cost_usd']].to_dict('records'),
            'summary': {
                'total_cost': round(selected_df['cost_usd'].sum(), 2),
                'avg_monthly_cost': round(selected_df['cost_usd'].sum(), 2),
                'most_expensive_service': {
                    'name': selected_df.groupby('service')['cost_usd'].sum().idxmax(),
                    'cost': round(selected_df.groupby('service')['cost_usd'].sum().max(), 2)
                }
            }
        }
        
        return jsonify(data)

    except Exception as e:
        print(f"Error processing data for {month}: {str(e)}")
        return jsonify({
            'error': f'Error processing data: {str(e)}',
            'service_costs': {},
            'monthly_trend': {'months': [], 'costs': []},
            'top_resources': [],
            'summary': {
                'total_cost': 0,
                'avg_monthly_cost': 0,
                'most_expensive_service': {'name': '-', 'cost': 0}
            }
        })

@app.route('/api/costs/<month>/<service>')
def get_service_costs(month, service):
    try:
        # Map month to corresponding file
        file_mapping = {
            'feb-2025': 'all_costs.csv',
            'jan-2025': 'all_costs-jan.csv',
            'dec-2024': 'all_costs-dec.csv'
        }
        
        # Get data for all months
        all_data = []
        for m, file in file_mapping.items():
            try:
                if os.path.exists(file):
                    df = pd.read_csv(file)
                    if not df.empty:
                        df['month'] = m
                        # Filter for specific service
                        service_df = df[df['service'] == service]
                        if not service_df.empty:
                            all_data.append(service_df)
            except Exception as e:
                print(f"Error reading file for {m}: {str(e)}")
                continue
        
        if not all_data:
            return jsonify({
                'months': [],
                'costs': [],
                'error': f'No data available for {service}'
            })
        
        # Combine all months data
        trend_df = pd.concat(all_data)
        
        # Calculate monthly totals for the service
        monthly_costs = trend_df.groupby('month')['cost_usd'].sum().round(2)
        
        return jsonify({
            'months': monthly_costs.index.tolist(),
            'costs': monthly_costs.values.tolist()
        })

    except Exception as e:
        print(f"Error processing service data: {str(e)}")
        return jsonify({
            'months': [],
            'costs': [],
            'error': str(e)
        })

@app.route('/api/costs/<month>/<service>/details')
def get_service_details(month, service):
    try:
        file_mapping = {
            'feb-2025': 'all_costs.csv',
            'jan-2025': 'all_costs-jan.csv',
            'dec-2024': 'all_costs-dec.csv'
        }
        
        filename = file_mapping.get(month)
        if not filename or not os.path.exists(filename):
            return jsonify({'error': f'No data available for {month}'})
            
        df = pd.read_csv(filename)
        service_df = df[df['service'] == service]
        
        if service_df.empty:
            return jsonify({'columns': [], 'resources': []})
            
        # Define columns to show for each service
        service_columns = {
            'CloudWatch': [
                {'key': 'log_group_name', 'label': 'Log Group'},
                {'key': 'size_gb', 'label': 'Size (GB)'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'ECR': [
                {'key': 'repository_name', 'label': 'Repository'},
                {'key': 'size_gb', 'label': 'Size (GB)'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'EC2': [
                {'key': 'instance_type', 'label': 'Instance Type'},
                {'key': 'usage_type', 'label': 'Usage Type'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'RDS': [
                {'key': 'usage_name', 'label': 'Usage'},
                {'key': 'usage_quantity', 'label': 'Quantity'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'EKS': [
                {'key': 'cluster_name', 'label': 'Cluster'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'DynamoDB': [
                {'key': 'table_name', 'label': 'Table'},
                {'key': 'size_gb', 'label': 'Size (GB)'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'S3': [
                {'key': 'name', 'label': 'Bucket'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'KMS': [
                {'key': 'key_id', 'label': 'Key ID'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'VPC': [
                {'key': 'vpc_id', 'label': 'VPC ID'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'EC2-Others-NAT': [
                {'key': 'nat_id', 'label': 'NAT Gateway'},
                {'key': 'data_gb', 'label': 'Data Processed (GB)'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ],
            'EC2-Others-EBS': [
                {'key': 'volume_id', 'label': 'Volume ID'},
                {'key': 'size_gb', 'label': 'Size (GB)'},
                {'key': 'cost_usd', 'label': 'Cost (USD)'}
            ]
        }
        
        columns = service_columns.get(service, [{'key': 'cost_usd', 'label': 'Cost (USD)'}])
        
        resources = []
        for _, row in service_df.iterrows():
            resource = {}
            for col in columns:
                value = row.get(col['key'], '')
                if col['key'] == 'cost_usd' or 'size' in col['key'] or 'quantity' in col['key']:
                    value = round(float(value), 2) if value != '' else 0
                resource[col['key']] = value
            resources.append(resource)
            
        return jsonify({
            'columns': columns,
            'resources': resources
        })
        
    except Exception as e:
        print(f"Error getting service details: {str(e)}")
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True) 