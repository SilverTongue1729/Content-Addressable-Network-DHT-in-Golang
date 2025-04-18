# Root Makefile that forwards commands to can-dht/Makefile

.PHONY: all
all:
	@cd can-dht && $(MAKE) all

.PHONY: build
build:
	@cd can-dht && $(MAKE) build

.PHONY: demo-cache
demo-cache:
	@cd can-dht && $(MAKE) demo-cache

.PHONY: visualizer
visualizer:
	@cd can-dht && $(MAKE) visualizer

.PHONY: help
help:
	@echo "This is a wrapper Makefile that forwards commands to can-dht/Makefile"
	@echo "Run 'cd can-dht && make help' for more information"
	@echo ""
	@echo "Available targets:"
	@echo "  all            : Forward to can-dht/Makefile"
	@echo "  build          : Forward to can-dht/Makefile"
	@echo "  demo-cache     : Run the cache demo"
	@echo "  visualizer     : Start the visualization tool"
	@echo "  help           : Show this help message" 