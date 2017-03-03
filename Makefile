.PHONY: e2e

test:
	go test -test.v ./...

e2e: docker_up
	E2E="true" DB_DSN="postgres://testing:testing@localhost:5439/testing?sslmode=disable" $(MAKE) test; $(MAKE) docker_down

.PHONY: docker_up
docker_up:
	@echo "setting up..."
	@docker rm -f e2e_pg || true
	@docker run -p 5439:5432 --name e2e_pg  -e POSTGRES_USER=testing -e POSTGRES_PASSWORD=testing -d postgres:9.5 && sleep 10

.PHONY: docker_down
docker_down:
	@echo "tearing down..."
	@docker stop e2e_pg || true