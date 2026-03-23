from google.cloud import bigquery
import json

def run():
    client = bigquery.Client(project='yelp-490821')
    
    # Drop the misconfigured Silver tables so they recreate with the correct partitioning
    client.delete_table('yelp-490821.silver.reviews', not_found_ok=True)
    client.delete_table('yelp-490821.silver.users', not_found_ok=True)
    print("Dropped silver.reviews and silver.users tables successfully.")

    # Show the inferred generic types from the Bronze external table
    table = client.get_table('yelp-490821.bronze.user')
    for f in table.schema:
        if f.name in ('elite', 'friends'):
            print(f"Bronze user {f.name}: {f.field_type} {f.mode}")

if __name__ == '__main__':
    run()
