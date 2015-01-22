rm -f cov.out
go test -coverprofile cov.out
go tool cover -html cov.out
