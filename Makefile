.PHONY: shellcheck

# Lint all shell scripts in bin/ using shellcheck
shellcheck:
	@echo "==> Running shellcheck on bin/*.sh..."
	shellcheck bin/*.sh
	@echo "==> Shellcheck completed."
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

docker-run:
	$(ECHO) Running Docker container...
	@docker run -it --rm bdns-ingest
	$(ECHO) Done.

docker-rmi:
	$(ECHO) Removing Docker image...
	@docker rmi bdns-ingest
	$(ECHO) Done.

.PHONY: install uninstall
