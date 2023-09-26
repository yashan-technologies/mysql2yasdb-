# env defines
GOOS=$(shell go env GOOS)
ARCH=$(shell arch)
VERSION=$(shell cat VERSION)
OS=$(if $(GOOS),$(GOOS),linux)

# go command defines
GO_BUILD=go build
GO_MOD_TIDY=$(go mod tidy -compat 1.19)

GO_BUILD_WITH_INFO=$(GO_BUILD) -ldflags "\
	-X 'main.version=$(VERSION)'"
	

# package defines
PKG_PERFIX=mysql2yasdb-$(VERSION)
PKG=$(PKG_PERFIX)-$(OS)-$(ARCH).tar.gz

BUILD_PATH=./build
PKG_PATH=$(BUILD_PATH)/$(PKG_PERFIX)

# build defines
BIN_M2Y=$(BUILD_PATH)/mysql2yasdb

LIB_PATH=$(PKG_PATH)/scripts

FILE_TO_COPY=./lib db.ini README.md $(BIN_M2Y)

.PHONY: clean force go_build

build: go_build
	@mkdir -p $(PKG_PATH) 
	@cp -r $(FILE_TO_COPY) $(PKG_PATH)
	@cd $(BUILD_PATH);tar -cvzf $(PKG) $(PKG_PERFIX)/

clean:
	rm -rf $(BUILD_PATH)

go_build: 
	$(GO_MOD_TIDY)
	$(GO_BUILD_WITH_INFO) -o $(BIN_M2Y) ./mysql2yasdb.go

force: clean build