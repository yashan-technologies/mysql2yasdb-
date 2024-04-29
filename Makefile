# env defines
GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)
ARCH_AMD=x86_64
ARCH_ARM=aarch64
OS=$(shell if [ $(GOOS)a != ""a ]; then echo $(GOOS); else echo "linux"; fi)
ARCH=$(shell if [ $(GOARCH)a == "arm64"a ]; then echo $(ARCH_ARM); else echo $(ARCH_AMD); fi)
VERSION=$(shell cat ./VERSION)
GO_VERSION=$(shell go env GOVERSION)
GIT_COMMIT_ID=$(shell git rev-parse HEAD)
GIT_DESCRIBE=$(shell git describe --always)

# go command defines
GO_BUILD=go build
GO_MOD_TIDY=$(go mod tidy -compat 1.19)

GO_BUILD_WITH_INFO=$(GO_BUILD) -ldflags "\
	-X 'm2y/defs/compiledef._appVersion=$(VERSION)' \
	-X 'm2y/defs/compiledef._goVersion=$(GO_VERSION)'\
	-X 'm2y/defs/compiledef._gitCommitID=$(GIT_COMMIT_ID)'\
	-X 'm2y/defs/compiledef._gitDescribe=$(GIT_DESCRIBE)'"
	

# package defines
PKG_PERFIX=mysql2yasdb-$(VERSION)
PKG=$(PKG_PERFIX)-$(OS)-$(ARCH).tar.gz

BUILD_PATH=./build
PKG_PATH=$(BUILD_PATH)/$(PKG_PERFIX)
BIN_PATH=$(PKG_PATH)/bin
LOG_PATH=$(PKG_PATH)/log
DOCS_PATH=$(PKG_PATH)/docs

# build defines
BIN_M2Y=$(BUILD_PATH)/mysql2yasdb
BIN_FILES=$(BIN_M2Y)

FILE_TO_COPY=./config ./lib
DIR_TO_MAKE=$(BIN_PATH) $(LOG_PATH) $(DOCS_PATH)

.PHONY: clean force go_build

build: go_build
	@mkdir -p $(DIR_TO_MAKE) 
	@cp -r $(FILE_TO_COPY) $(PKG_PATH)
	@cp ./README.md $(DOCS_PATH)
	@mv $(BIN_FILES) $(BIN_PATH)
	@> $(LOG_PATH)/mysql2yasdb.log
	@> $(LOG_PATH)/console.out
	@cd $(PKG_PATH);ln -s ./bin/mysql2yasdb ./mysql2yasdb
	@cd $(BUILD_PATH);tar -cvzf $(PKG) $(PKG_PERFIX)/

clean:
	rm -rf $(BUILD_PATH)

go_build: 
	$(GO_MOD_TIDY)
	$(GO_BUILD_WITH_INFO) -o $(BIN_M2Y) ./cmd/*.go

force: clean build