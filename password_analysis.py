import pandas as pd
import csv
import password_checker


def process_chunk(chunk):
    chunk_filter = chunk.dropna()
    chunk_filter = chunk_filter[chunk_filter["password"].str.len() >= 8]
    chunk_filter["n_hacked"] = (
        chunk_filter["password"].apply(password_checker.pwned_api_check).astype("int")
    )
    return chunk_filter


def main():
    input_filename = "./data/10millionPasswords.csv"
    output_filename = "./results/passwords_ranked_worst_to_best.csv"

    # Read the CSV file into a pandas DataFrame in chunks
    chunk_size = 100
    chunks = pd.read_csv(input_filename, chunksize=chunk_size)

    first_chunk = True

    # Process chunks and append results to the output CSV
    with open(output_filename, "a", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)

        for chunk in chunks:
            processed_chunk = process_chunk(chunk)

            # Append chunk data to the CSV file
            processed_chunk.to_csv(csvfile, header=first_chunk, index=False)
            first_chunk = False


if __name__ == "__main__":
    main()


# import pandas as pd
# import csv
# from multiprocessing import Pool
# import password_checker

# # Define the number of parts to split the dataset
# num_parts = 100

# # def process_chunk(chunk):
# #     # Count characters and create a new column for character count
# #     chunk['Character Count'] = chunk['Password'].apply(len)
# #     return chunk


# def main():
#     input_filename = "./data/10millionPasswords.csv"
#     output_filename = "./results/passwords_ranked_worst_to_best.csv"

#     # Read the CSV file into a pandas DataFrame in chunks
#     chunk_size = int(1e5)  # Adjust this based on your system's memory capacity
#     chunks = pd.read_csv(input_filename, chunksize=chunk_size)

#     # Create a multiprocessing pool for parallel processing
#     with Pool() as pool:
#         # Process chunks in parallel and append results to the output CSV
#         with open(output_filename, "a", newline="", encoding="utf-8") as csvfile:
#             csv_writer = csv.writer(csvfile)
#             first_chunk = True

#             for chunk in pool.imap(password_checker.main(), chunks):
#                 # Append chunk data to the CSV file
#                 chunk.to_csv(csvfile, header=first_chunk, index=False)
#                 first_chunk = False


# if __name__ == "__main__":
#     main()
