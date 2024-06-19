import psycopg2


# Connect to your PostgreSQL database
def connect_to_postgres(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        return conn
    
    except psycopg2.Error as e:
    
        return None






def insert_data_into_db():

    dbname = 'BCS_items'
    user = 'postgres'
    password = 'post@BCS'
    host = 'localhost' 
    port = '5432'  # Default PostgreSQL port is 5432


    conn = connect_to_postgres(dbname, user, password, host, port)

    # mapping the curson through connection
    cursor = conn.cursor()

    location_id = "temp"
    location_name = "temp"
    inv_mast_uid = "temp"
    item_id = "temp"
    item_desc = "temp"
    on_vendor_price_book = "temp"
    product_type = "temp"
    primary_supplier_id = "temp"
    supplier_name = "temp"
    replenishment_method = "temp"
    replenishment_location = "temp"
    inv_min = "temp"
    inv_max = "temp"
    stockable = "temp"
    sellable = "temp"
    buyable = "temp"
    qty_on_hand = "temp"
    track_bins = "temp"
    primary_bin = "temp"
    repl_loc_review = "temp"
    repl_meth_review = "temp"
    track_bin_review = "temp"
    discrepancy_type = "temp"
    


    sql = """
    INSERT INTO replenishment_items (location_id, location_name, inv_mast_uid, item_id, item_desc,
                                on_vendor_price_book, product_type, primary_supplier_id, supplier_name,
                                replenishment_method, replenishment_location, inv_min, inv_max,
                                stockable, sellable, buyable, qty_on_hand, track_bins, primary_bin,
                                repl_loc_review, repl_meth_review, track_bin_review, discrepancy_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    print(sql)


    # Execute the INSERT statement with the values from the current row
    cursor.execute(sql, (location_id, location_name, inv_mast_uid, item_id, item_desc,
                        on_vendor_price_book, product_type, primary_supplier_id, supplier_name,
                        replenishment_method, replenishment_location, inv_min, inv_max,
                        stockable, sellable, buyable, qty_on_hand, track_bins, primary_bin,
                        repl_loc_review, repl_meth_review, track_bin_review, discrepancy_type))

    # Commit the transaction to persist the changes
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == "__main__" :

    insert_data_into_db()