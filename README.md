# Blu-ray Backup Manager

A Terminal User Interface (TUI) application for managing and cataloging file backups to Blu-ray discs with multi-session support, searchable database, and UDF filesystem burning.

![Python Version](https://img.shields.io/badge/python-3.7+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Tests](https://img.shields.io/badge/tests-34%20passing-brightgreen.svg)

## Features

### 📀 Multi-Session Burning

- Append files to existing discs until full
- Automatic space tracking and validation
- Support for 25GB, 50GB, and 100GB Blu-ray discs
- Queue system to stage files before burning

### 🔍 Searchable Database

- SQLite database tracks every file
- Search by filename or path
- See exactly which disc contains any file
- Individual file tracking even for directories

### 💾 UDF Filesystem

- Files burned as raw files (not ISO)
- No extraction needed - browse like a USB drive
- Compatible with Windows, macOS, and Linux
- Direct file restoration

### 🎯 Clean Architecture

- Separation of business logic and UI
- Comprehensive unit tests (34 tests)
- Type hints throughout
- Well-documented code

## Installation

### Requirements

**Python Requirements:**

- Python 3.7 or higher
- [Textual](https://github.com/Textualize/textual) TUI library

**Burning Tools (Optional - for actual burning):**

- **Linux:** `growisofs` and `mkisofs`/`genisoimage`
- **macOS:** Built-in tools (hdiutil/drutil)

### Quick Install

**Using pip (recommended):**

```bash
# Using pipx (isolated environment)
sudo apt install pipx  # or brew install pipx on macOS
pipx install textual
python3 bluray_backup.py

# Or using a virtual environment
python3 -m venv bluray-env
source bluray-env/bin/activate
pip install textual
python3 bluray_backup.py
```

**Using system packages (Ubuntu/Debian):**

```bash
sudo apt install python3-textual
python3 bluray_backup.py
```

**Install burning tools (Linux):**

```bash
sudo apt install growisofs genisoimage
```

## Usage

### Starting the Application

```bash
# Run the application
python3 bluray_backup.py

# Run tests
python3 bluray_backup.py --test

# Make executable (optional)
chmod +x bluray_backup.py
./bluray_backup.py
```

### Keyboard Shortcuts

|Key  |Action                 |
|-----|-----------------------|
|`a`  |Add a new disk manually|
|`b`  |Open burn queue        |
|`s`  |Search for files       |
|`q`  |Quit application       |
|`Esc`|Go back / Cancel       |

### Workflow

#### 1. Add Files to Queue

1. Press `b` to open Burn Queue
1. Click “Add Files”
1. Enter file or directory path
1. Files are added with automatic size calculation

> **Tip:** Wildcards are supported! Enter a pattern like `/home/user/photos/*.jpg` to queue multiple files at once. The `~` home directory shortcut is also expanded automatically.

#### 2. Burn to Disc

1. In Burn Queue, click “Burn to Disc”
1. **Select existing disk** from the table (for multi-session)
- Or enter new disk label to create new disc
1. Set capacity (25, 50, or 100 GB for new discs)
1. Click “Start Burn”
1. Files are burned and automatically cataloged

#### 3. Search for Files

1. Press `s` to open Search
1. Enter filename or path fragment
1. View results showing:
- Original file location
- Location on disc
- Which disc contains it
- Backup date and size

#### 4. Restore Files

1. Search for the file you need
1. Note which disc label contains it
1. Insert that disc
1. Copy files directly from the mounted disc

## Architecture

### Project Structure

```
bluray_backup.py
├── Core Business Logic
│   ├── Database (SQLite operations)
│   ├── FileSystemHelper (File operations)
│   └── BurnEngine (Burning operations)
├── Unit Tests (34 comprehensive tests)
└── TUI Layer
    ├── Main Application
    ├── Search Screen
    ├── Queue Screen
    ├── Burn Screen
    └── Add Disk Screen
```

### Database Schema

**Disks Table:**

- Tracks disc label, capacity, used space, creation date, notes

**Files Table:**

- Tracks original path, disc path, size, backup date
- Foreign key to disks table
- Indexed for fast searching

**Burn Queue Table:**

- Temporary staging area for files to burn

## Examples

### Example 1: Initial Backup

```bash
# Start the app
python3 bluray_backup.py

# Add files to queue
Press 'b' → Add Files → /home/user/photos

# Burn to new disc
Burn to Disc → Enter "BACKUP-2024-001" → Capacity: 25 → Start Burn

# Result: New 25GB disc with 5.2GB used, 19.8GB free
```

### Example 2: Multi-Session Append

```bash
# Later, add more files
Press 'b' → Add Files → /home/user/documents

# Append to existing disc
Burn to Disc → Click "BACKUP-2024-001" from table → Start Burn

# Result: Same disc now has 12.7GB used, 12.3GB free
```

### Example 3: Search and Restore

```bash
# Find a file
Press 's' → Enter "report.pdf"

# Results show:
# Original: /home/user/documents/report.pdf
# On Disc: documents/report.pdf
# Disk: BACKUP-2024-001

# Restore: Insert BACKUP-2024-001 and copy the file
```

## Testing

Run the comprehensive test suite:

```bash
python3 bluray_backup.py --test
```

**Test Coverage:**

- Database operations (17 tests)
- File system operations (4 tests)
- Burn engine operations (7 tests)
- Input validation and edge cases

## Troubleshooting

### “externally-managed-environment” error

**Ubuntu/Debian users:** Use pipx or virtual environment (see Installation)

### “No burning tool found”

**Linux:** Install growisofs

```bash
sudo apt install growisofs genisoimage
```

**macOS:** Built-in tools should work automatically

### Search shows files but table is blank

Press `Esc` once to refresh, or upgrade to latest Textual version

### Cannot import ComposeResult

You have an older Textual version. The code is compatible, but consider upgrading:

```bash
pip install --upgrade textual
```

### Burn is slow / taking 30–45 minutes

This is expected. The app burns at 4x speed intentionally — higher speeds (up to 6x) are supported by the hardware but are prone to write errors and disc failures. Patience here means a reliable backup.

## Technical Details

### Why UDF Instead of ISO?

- **Direct Access:** Browse files without extraction
- **Multi-Session:** Append files to existing discs
- **Compatibility:** Works on all major operating systems
- **Simplicity:** Users understand “copy file from disc”

### Why SQLite?

- **Zero Configuration:** No server required
- **Single File:** Easy backup of database
- **ACID Transactions:** Data integrity guaranteed
- **Fast Searches:** Indexed queries even with 100k+ files
- **Portable:** Works anywhere Python works

### Why Not JSON/XML/MongoDB?

See the SQLite vs JSON vs MongoDB comparison in the docs - SQLite is ideal for this single-user, local use case.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
1. Create a feature branch
1. Add tests for new functionality
1. Ensure all tests pass
1. Submit a pull request

## License

MPT 2.0 License - See LICENSE file for details

## Roadmap

Potential future enhancements:

- [ ] SHA-256 checksums for file verification
- [ ] Duplicate file detection
- [ ] Export catalog to JSON/CSV
- [ ] Disc verification after burn
- [ ] Support for DVD (4.7GB) discs
- [ ] Automatic disc spanning for large files
- [ ] Integration with cloud backup services

## Acknowledgments

- Built with [Textual](https://github.com/Textualize/textual) by Textualize
- Inspired by the need for simple, reliable physical backups

## Support

For issues, questions, or suggestions:

- Open an issue on GitHub
- Include Python version and OS
- Include output of `python3 bluray_backup.py --test`

-----

**Made with ❤️ for people who believe in physical backups**
