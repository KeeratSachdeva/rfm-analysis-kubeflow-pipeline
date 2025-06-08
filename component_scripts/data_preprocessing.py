import pandas as pd

def data_preprocessing(data_gcs_uri: str, product_details_gcs_uri: str):

    '''
    # Downloading the raw data from GCS.
    bucket = client.bucket('')
    source_blob = bucket.blob(input_path)
    source_blob.download_to_filename('data.csv')
    '''

    # Data Preprocessing.
    # Loading the dataset.
    # df = pd.read_csv('data.csv', encoding = 'ISO-8859-1')
    df = pd.read_csv(data_gcs_uri, encoding = 'ISO-8859-1')

    # Product IDs/StockCodes with no or multiple Description(s).
    df.drop(df[df['StockCode'].isin(df.groupby('StockCode')['Description'].nunique()[df.groupby('StockCode')['Description'].nunique() != 1].index)].index, inplace = True)

    # Product IDs/Stock Codes with the same Description. 
    df['StockCode'] = df['StockCode'].apply(lambda product_id: product_id.upper())
    df.drop(df[df['StockCode'].isin(df[df['Description'].isin(df[~df['Description'].isna()].groupby('Description')['StockCode'].nunique()[df[~df['Description'].isna()].groupby('Description')['StockCode'].nunique() > 1].index)]['StockCode'].unique())].index, inplace = True)

    # Missing Descriptions.
    products = df[['StockCode', 'Description']].dropna().drop_duplicates().set_index('StockCode').copy()
    df['Description'] = df['StockCode'].apply(lambda product_id: products.loc[product_id, 'Description'])    

    # InvoiceDate.
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    # Invalid entries. Negative UnitPrice.
    df.drop(df[df['UnitPrice'] < 0].index, inplace = True)

    # Outliers.
    df.drop(df[(df['Quantity'] < -60) | (df['Quantity'] > 60)].index, inplace = True)
    df.drop(df[df['UnitPrice'] > 15].index, inplace = True)

    # Logged-out Customers.
    df.dropna(inplace = True)
    df['CustomerID'] = df['CustomerID'].astype(int)

    # Data Augmentation.
    '''
    blob = bucket.blob('data/product_details.xlsx')
    blob.download_to_filename('product_details.xlsx')
    product_details = pd.read_excel('product_details.xlsx')
    '''
    product_details = pd.read_excel(product_details_gcs_uri)

    df['Description'] = df['Description'].apply(lambda descr: ' '.join([word.strip() for word in descr.strip().split()]))

    # Rectifying a few spelling mistakes.
    correct_product_names = {
        'LIGHT GARLAND BUTTERFILES PINK': 'LIGHT GARLAND BUTTERFLIES PINK',
        'BLUE ROSE PATCH PURSE PINK BUTTERFL': 'BLUE ROSE PATCH PURSE PINK BUTTERFLY',
        'MIDNIGHT BLUE COPPER FLOWER NECKLAC': 'MIDNIGHT BLUE COPPER FLOWER NECKLACE',
        'AMETHYST CHUNKY BEAD BRACELET W STR': 'AMETHYST CHUNKY BEAD BRACELET W STRAP',
        'SET/3 RABBITS FLOWER SKIPPPING ROPE': 'SET/3 RABBITS FLOWER SKIPPING ROPE',
        'UTILTY CABINET WITH HOOKS': 'UTILITY CABINET WITH HOOKS',
        'PEARL AND CHERRY QUARTZ BRACLET': 'PEARL AND CHERRY QUARTZ BRACELET',
        'WHITE VINT ART DECO CRYSTAL NECKLAC': 'WHITE VINT ART DECO CRYSTAL NECKLACE',
        'PURPLE FOXGLOVE ARTIIFCIAL FLOWER': 'PURPLE FOXGLOVE ARTIFICIAL FLOWER',
        'RASPBERRY ANT COPPER FLOWER NECKLAC': 'RASPBERRY ANT COPPER FLOWER NECKLACE'
    }
    df['Description'] = df['Description'].apply(lambda descr: correct_product_names[descr] if descr in correct_product_names else descr)
    df = pd.merge(df, product_details, on = 'Description', how = 'left')
    df.drop(df[df['Category'].isna()].index, inplace = True)

    df.reset_index(drop = True, inplace = True)

    # TotalPrice.
    df['TotalPrice'] = df['UnitPrice'] * df['Quantity']

    '''
    df.to_csv('processed_data.csv', index = False)

    # Uploading the cleaned and processed data to GCS.
    destination_blob = bucket.blob(output_path)
    destination_blob.upload_from_filename('processed_data.csv')
    '''
    # df.to_csv('gs://.../data/processed_data.csv', index = False)

    return df