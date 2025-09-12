# Makefile for bdns-ingest

PREFIX   ?= $(HOME)/.local
BINDIR   := $(PREFIX)/bin
SHAREDIR := $(PREFIX)/share/bdns-ingest

ECHO = @echo "==>"

install:
	$(ECHO) Installing bdns-ingest into $(PREFIX)

	$(ECHO) Copying application files...
	@install -d $(SHAREDIR)
	@cp -r bin schemas sql $(SHAREDIR)/
	@chmod +x $(SHAREDIR)/bin/*.sh

	$(ECHO) Creating symlinks in $(BINDIR)...
	@install -d $(BINDIR)
	@for f in $(SHAREDIR)/bin/*.sh; do \
		name=$$(basename "$$f" .sh); \
		ln -sf "$$f" "$(BINDIR)/$$name"; \
	done

	$(ECHO) Checking PATH...
	@if ! echo "$$PATH" | tr ':' '\n' | grep -qx "$(BINDIR)"; then \
		echo "WARNING: $(BINDIR) is not in your PATH."; \
		echo "Add this line to your shell config (e.g. ~/.bashrc or ~/.zshrc):"; \
		echo "  export PATH=\"$(BINDIR):\$$PATH\""; \
	fi

	$(ECHO) Done.

uninstall:
	$(ECHO) Removing symlinks from $(BINDIR)...
	@for f in $(SHAREDIR)/bin/*.sh; do \
		name=$$(basename "$$f" .sh); \
		rm -f "$(BINDIR)/$$name"; \
	done

	$(ECHO) Removing application files from $(SHAREDIR)...
	@rm -rf $(SHAREDIR)

	$(ECHO) Done.

docker-build:
	$(ECHO) Building Docker image...
	@docker build -t bdns-ingest .
	$(ECHO) Done.

shellcheck:
	$(ECHO) Running shellcheck on bin/*.sh...
	shellcheck bin/*.sh
	$(ECHO) Shellcheck completed.

test:
	$(ECHO) Running tests...
	@bats tests/
	$(ECHO) Tests completed.

.PHONY: install uninstall docker-build shellcheck test

