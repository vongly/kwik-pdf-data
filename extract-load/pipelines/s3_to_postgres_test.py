from s3_to_postgres import FullPipeline

if __name__ == '__main__':
    pipeline = FullPipeline(test=True)
    pipeline.run_all()
