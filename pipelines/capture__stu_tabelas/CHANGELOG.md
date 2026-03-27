# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-03-27

### Added

- Initial migration of STU tabelas capture flow from Prefect 1.4 to Prefect 3.0
- Support for all 21 STU tables
- STU-specific extractor implementation with delta processing
- Automated recapture with configurable timestamps
- Schedule: Daily at 8:00 AM UTC-3 (cron: "0 11 * * *")
