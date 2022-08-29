include .env

create_bucket:
	gcloud alpha storage buckets create gs://${BUCKET_NAME}

build_run:
	go build
	./federal_revenue

clean:
	rm -rf data/
