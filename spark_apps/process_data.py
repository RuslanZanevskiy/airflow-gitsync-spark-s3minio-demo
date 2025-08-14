from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder \
        .appName("AirflowSparkMinIO") \
        .getOrCreate()

    print("Spark Session создана успешно.")

    # Создаем тестовый DataFrame
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)
    
    print(f"Исходный DataFrame (создан в Spark):")
    df.show()
    
    # Путь для записи в MinIO
    # Убедитесь, что бакет 'landing-zone' существует
    output_path = "s3a://landing-zone/people_data"

    print(f"Запись DataFrame в MinIO по пути: {output_path}")

    # Записываем DataFrame в формате Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    print("Запись прошла успешно.")

    # Читаем данные обратно для проверки
    print(f"Чтение данных из MinIO по пути: {output_path}")
    df_read = spark.read.parquet(output_path)
    
    print("Данные, прочитанные из MinIO:")
    df_read.show()
    
    # Пример простой трансформации
    print("Трансформация: добавление столбца 'full_name'")
    df_transformed = df_read.withColumn("full_name", F.concat(F.col("firstname"), F.lit(" "), F.col("lastname")))
    
    transformed_output_path = "s3a://processed-zone/people_full_name"
    print(f"Запись трансформированных данных в: {transformed_output_path}")

    df_transformed.write.mode("overwrite").parquet(transformed_output_path)
    
    print("Трансформированные данные успешно записаны.")
    
    spark.stop()

if __name__ == "__main__":
    main()
