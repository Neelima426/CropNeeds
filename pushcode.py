import os
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud import exceptions

# Initialize Firebase Admin SDK
cred = credentials.Certificate('agri-app-9bf66-firebase-adminsdk-y57m7-0106b7f5ab.json')
firebase_admin.initialize_app(cred, {
    'storageBucket': 'agri-app-9bf66.appspot.com'
})

# Firestore and Storage references
db = firestore.client()
bucket = storage.bucket()

# Load the CSV file
file_path = 'products.csv'
df = pd.read_csv(file_path)


# Initialize a DataFrame to log errors
error_df = pd.DataFrame(columns=df.columns.tolist() + ['Error'])

def upload_image(image_name):
    for extension in ['jpeg', 'jpg', 'png']:
        try:
            blob = bucket.blob(f'product_images/{image_name}.{extension}')
            blob.upload_from_filename(f'assets/{image_name}.{extension}')
            blob.make_public()  # Consider the security implications
            return blob.public_url
        except FileNotFoundError:
            continue
        except exceptions.GoogleCloudError as e:
            print(f"Google Cloud error during image upload: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error during image upload: {e}")
            return None
    return None

# Clean the data if needed
df['Catagory'] = df['Catagory'].str.strip()
df['Brand'] = df['Brand'].str.strip()
df['Name'] = df['Name'].str.strip()
df['Discription'] = df['Discription'].str.strip()
df['Quantity'] = df['Quantity'].astype(str)
df['Price'] = df['Price'].astype(float)

# Upload data to Firestore
for index, row in df.iterrows():
    image_url = upload_image(row['image'])
    if image_url:
        product_data = {
            'category': row['Catagory'],
            'brand': row['Brand'],
            'name': row['Name'],
            'description': row['Discription'],
            'quantity': row['Quantity'],
            'price': row['Price'],
            'image': image_url
        }
        try:
            db.collection('products').add(product_data)
            print(f"Uploaded: {row['Name']}")
        except exceptions.GoogleCloudError as e:
            print(f"Failed to upload product data for {row['Name']} due to Google Cloud error: {e}")
        except Exception as e:
            print(f"Failed to upload product data for {row['Name']} due to an unexpected error: {e}")
    else:
        error_row = row.copy()
        error_row['Error'] = "Image not found"
        error_df = pd.concat([error_df, pd.DataFrame([error_row])], ignore_index=True)
        print(f"Error uploading {row['Name']}: Image not found")

# Save error log to a CSV file if there are errors
if not error_df.empty:
    error_file_path = 'error_log.csv'
    error_df.to_csv(error_file_path, index=False)
    print(f"Errors logged to {error_file_path}")

print('Data upload completed.')
