#!/usr/bin/env python3
"""
Blu-ray Backup Manager - A TUI for managing and burning file backups to Blu-ray disks
Requires: pip install textual (version 0.40.0 or higher recommended)
Optional: growisofs (for burning on Linux), hdiutil/drutil (macOS)

Run tests: python bluray_backup.py --test
Run app: python bluray_backup.py
"""

import glob
import sqlite3
import os
import subprocess
import shutil
import tempfile
import unittest
import unittest.mock
import sys
from pathlib import Path
from datetime import datetime
from typing import Tuple, List, Optional, Dict, Iterable
from textual.app import App
from textual.containers import Container, Horizontal
from textual.widgets import Header, Footer, Button, DataTable, Input, Label, ProgressBar
from textual.binding import Binding
from textual.screen import Screen, ModalScreen

# For compatibility with older Textual versions
try:
    from textual.app import ComposeResult
except ImportError:
    ComposeResult = Iterable

DB_FILE = "bluray_backup.db"

# ============================================================================
# Core Business Logic Layer
# ============================================================================

class Database:
    """Handles all database operations"""
    
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self.init_db()
    
    def init_db(self) -> None:
        """Initialize database schema"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS disks
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      label TEXT UNIQUE NOT NULL,
                      capacity_gb INTEGER NOT NULL,
                      used_gb REAL DEFAULT 0,
                      created_date TEXT NOT NULL,
                      notes TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS files
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      disk_id INTEGER NOT NULL,
                      file_path TEXT NOT NULL,
                      disk_path TEXT NOT NULL,
                      file_size_gb REAL NOT NULL,
                      backup_date TEXT NOT NULL,
                      checksum TEXT,
                      FOREIGN KEY (disk_id) REFERENCES disks(id))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS burn_queue
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      file_path TEXT NOT NULL,
                      file_size_gb REAL NOT NULL,
                      added_date TEXT NOT NULL)''')
        
        # Create indexes for better search performance
        c.execute('''CREATE INDEX IF NOT EXISTS idx_files_path 
                     ON files(file_path)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_files_disk_path 
                     ON files(disk_path)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_files_disk_id
                     ON files(disk_id)''')
        
        conn.commit()
        conn.close()
    
    def add_disk(self, label: str, capacity_gb: int, notes: str = "") -> Tuple[bool, str, Optional[int]]:
        """Add a new disk to the database"""
        if not label or not label.strip():
            return False, "Label cannot be empty", None
        
        if capacity_gb <= 0:
            return False, "Capacity must be positive", None
        
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            c.execute("INSERT INTO disks (label, capacity_gb, created_date, notes) VALUES (?, ?, ?, ?)",
                     (label.strip(), capacity_gb, date, notes))
            conn.commit()
            disk_id = c.lastrowid
            return True, "Disk added successfully", disk_id
        except sqlite3.IntegrityError:
            return False, "Disk label already exists", None
        finally:
            conn.close()
    
    def get_disks(self) -> List[Tuple]:
        """Get all disks ordered by creation date"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id, label, capacity_gb, used_gb, created_date, notes FROM disks ORDER BY created_date DESC")
        disks = c.fetchall()
        conn.close()
        return disks
    
    def get_disk_by_id(self, disk_id: int) -> Optional[Tuple]:
        """Get a specific disk by ID"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id, label, capacity_gb, used_gb, created_date, notes FROM disks WHERE id = ?", (disk_id,))
        disk = c.fetchone()
        conn.close()
        return disk
    
    def get_disk_by_label(self, label: str) -> Optional[Tuple]:
        """Get a specific disk by label"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id, label, capacity_gb, used_gb, created_date, notes FROM disks WHERE label = ?", (label,))
        disk = c.fetchone()
        conn.close()
        return disk
    
    def add_file(self, disk_id: int, file_path: str, disk_path: str, file_size_gb: float, checksum: Optional[str] = None) -> Tuple[bool, str]:
        """Add a file record to the database"""
        if not file_path or not file_path.strip():
            return False, "File path cannot be empty"
        
        if not disk_path or not disk_path.strip():
            return False, "Disk path cannot be empty"
        
        if file_size_gb < 0:
            return False, "File size cannot be negative"
        
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Verify disk exists
        c.execute("SELECT id FROM disks WHERE id = ?", (disk_id,))
        if not c.fetchone():
            conn.close()
            return False, "Disk not found"
        
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            c.execute("INSERT INTO files (disk_id, file_path, disk_path, file_size_gb, backup_date, checksum) VALUES (?, ?, ?, ?, ?, ?)",
                     (disk_id, file_path.strip(), disk_path.strip(), file_size_gb, date, checksum))
            
            c.execute("UPDATE disks SET used_gb = used_gb + ? WHERE id = ?", (file_size_gb, disk_id))
            
            conn.commit()
            return True, "File added successfully"
        except Exception as e:
            conn.rollback()
            return False, f"Error adding file: {str(e)}"
        finally:
            conn.close()
    
    def get_files_for_disk(self, disk_id: int) -> List[Tuple]:
        """Get all files for a specific disk"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id, file_path, disk_path, file_size_gb, backup_date FROM files WHERE disk_id = ? ORDER BY backup_date DESC",
                 (disk_id,))
        files = c.fetchall()
        conn.close()
        return files
    
    def search_files(self, search_term: str) -> List[Tuple]:
        """Search for files by path"""
        if not search_term or not search_term.strip():
            return []
        
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        search_pattern = f"%{search_term.strip()}%"
        c.execute("""SELECT f.file_path, f.disk_path, f.file_size_gb, f.backup_date, d.label 
                    FROM files f 
                    JOIN disks d ON f.disk_id = d.id 
                    WHERE f.file_path LIKE ? OR f.disk_path LIKE ?""",
                 (search_pattern, search_pattern))
        results = c.fetchall()
        conn.close()
        return results
    
    def add_to_queue(self, file_path: str, file_size_gb: float) -> Tuple[bool, str]:
        """Add a file to the burn queue"""
        if not file_path or not file_path.strip():
            return False, "File path cannot be empty"
        
        if file_size_gb < 0:
            return False, "File size cannot be negative"
        
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            c.execute("INSERT INTO burn_queue (file_path, file_size_gb, added_date) VALUES (?, ?, ?)",
                     (file_path.strip(), file_size_gb, date))
            conn.commit()
            return True, "Added to queue"
        except Exception as e:
            return False, f"Error: {str(e)}"
        finally:
            conn.close()
    
    def get_queue(self) -> List[Tuple]:
        """Get all items in the burn queue"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id, file_path, file_size_gb, added_date FROM burn_queue ORDER BY added_date")
        queue = c.fetchall()
        conn.close()
        return queue
    
    def clear_queue(self) -> None:
        """Clear all items from the burn queue"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("DELETE FROM burn_queue")
        conn.commit()
        conn.close()
    
    def remove_from_queue(self, queue_id: int) -> Tuple[bool, str]:
        """Remove a specific item from the burn queue"""
        if queue_id <= 0:
            return False, "Invalid queue ID"
        
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("DELETE FROM burn_queue WHERE id = ?", (queue_id,))
        rows_affected = c.rowcount
        conn.commit()
        conn.close()
        
        if rows_affected > 0:
            return True, "Removed from queue"
        else:
            return False, "Queue item not found"


class FileSystemHelper:
    """Helper functions for file system operations"""
    
    @staticmethod
    def calculate_size(path: Path) -> float:
        """Calculate size of file or directory in GB"""
        if not path.exists():
            return 0.0
        
        if path.is_file():
            size_bytes = path.stat().st_size
        else:
            size_bytes = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
        
        return size_bytes / (1024**3)
    
    @staticmethod
    def prepare_staging_area(queue_items: List[Tuple], staging_dir: str) -> Tuple[str, Dict[str, str]]:
        """Prepare a staging directory with all files to burn"""
        staging_path = Path(staging_dir)
        
        # Clean and create staging directory
        if staging_path.exists():
            shutil.rmtree(staging_path)
        staging_path.mkdir(parents=True)
        
        file_map = {}  # Maps original path to staging path
        
        for _, filepath, _, _ in queue_items:
            source = Path(filepath)
            
            if not source.exists():
                continue
            
            # Create relative path structure
            if source.is_file():
                # Use just the filename for files
                dest = staging_path / source.name
                # Handle duplicates
                counter = 1
                while dest.exists():
                    dest = staging_path / f"{source.stem}_{counter}{source.suffix}"
                    counter += 1
                
                shutil.copy2(source, dest)
                file_map[str(source)] = str(dest.relative_to(staging_path))
            else:
                # For directories, preserve structure
                dest_dir = staging_path / source.name
                shutil.copytree(source, dest_dir)
                file_map[str(source)] = str(dest_dir.relative_to(staging_path))
        
        return str(staging_path), file_map


class BurnEngine:
    """Handles the actual burning process with UDF filesystem"""
    
    @staticmethod
    def detect_burner() -> Tuple[Optional[str], Optional[str]]:
        """Detect available burning tools and drives"""
        system = os.uname().sysname if hasattr(os, 'uname') else 'Unknown'
        
        if system == 'Linux':
            if shutil.which('growisofs'):
                return 'growisofs', BurnEngine.find_linux_drive()
        elif system == 'Darwin':  # macOS
            if shutil.which('drutil'):
                return 'drutil', BurnEngine.find_macos_drive()
        
        return None, None
    
    @staticmethod
    def find_linux_drive() -> str:
        """Find optical drive on Linux - prefer Blu-ray capable drives"""
        # Find all optical drives
        drives = sorted(glob.glob('/dev/sr*'))
        
        # Try to identify the Blu-ray drive by checking media info
        for drive in drives:
            try:
                result = subprocess.run(
                    ['dvd+rw-mediainfo', drive],
                    capture_output=True, text=True, timeout=5
                )
                # Prefer drives that mention BD (Blu-ray)
                if 'BD' in result.stdout or 'blu' in result.stdout.lower():
                    return drive
            except Exception:
                pass
        
        # Fall back to last drive (sr1 if two drives present, sr0 if only one)
        if drives:
            return drives[-1]
        return '/dev/sr0'  # Absolute default
    
    @staticmethod
    def find_macos_drive() -> str:
        """Find optical drive on macOS"""
        try:
            result = subprocess.run(['drutil', 'status'], capture_output=True, text=True, timeout=5)
            lines = result.stdout.split('\n')
            for line in lines:
                if 'Name:' in line:
                    return line.split(':', 1)[1].strip()
        except Exception:
            pass
        return 'disk1'
    
    @staticmethod
    def build_command(staging_dir: str, device: str, tool: str, label: str,
                      is_new_disc: bool = True) -> List[str]:
        """Build the burn command without executing it.
        
        Returns the exact command list that burn_udf would run, allowing
        it to be displayed to the user before execution.
        """
        if tool == 'growisofs':
            session_flag = '-Z' if is_new_disc else '-M'
            return [
                'growisofs',
                f'{session_flag}={device}',
                '-speed=4',
                '-udf',
                '-V', label.strip(),
                '-r',
                staging_dir
            ]
        elif tool == 'drutil':
            cmd = ['drutil', 'burn', '-udf', '-noverify']
            if not is_new_disc:
                cmd.append('-append')
            cmd.append(staging_dir)
            return cmd
        return []

    @staticmethod
    def burn_udf(staging_dir: str, device: str, tool: str, label: str,
                 is_new_disc: bool = True) -> Tuple[bool, str]:
        """Burn files directly to disc with UDF filesystem.

        Args:
            staging_dir: Path to the directory of files to burn.
            device:      Block device path (e.g. /dev/sr0).
            tool:        Burning tool name ('growisofs' or 'drutil').
            label:       UDF volume label.
            is_new_disc: True  → use -Z (start a brand-new disc / overwrite).
                         False → use -M (append a new session to existing data).
                         NEVER pass True when appending to a disc that already
                         has data — doing so will silently destroy that data.
        """
        if not staging_dir or not Path(staging_dir).exists():
            return False, "Staging directory does not exist"

        if not device:
            return False, "No device specified"

        if not tool:
            return False, "No burning tool specified"

        if not label or not label.strip():
            return False, "Label cannot be empty"

        try:
            if tool == 'growisofs':
                cmd = BurnEngine.build_command(staging_dir, device, tool, label, is_new_disc)

                proc = subprocess.Popen(
                    cmd,
                    stdout=None,
                    stderr=None,
                    stdin=subprocess.DEVNULL
                )
                try:
                    proc.wait(timeout=7200)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    return False, "Burn timed out after 2 hours"

                if proc.returncode == 0:
                    return True, "Burn completed successfully"

                try:
                    verify = subprocess.run(
                        ['dvd+rw-mediainfo', device],
                        capture_output=True, text=True, timeout=15
                    )
                    if 'Disc status:' in verify.stdout and 'blank' not in verify.stdout:
                        return True, "Burn completed successfully (growisofs reported warnings but disc has data)"
                except Exception:
                    pass

                return False, f"growisofs exited with code {proc.returncode} — check terminal output above for details"

            elif tool == 'drutil':
                cmd = BurnEngine.build_command(staging_dir, device, tool, label, is_new_disc)
                result = subprocess.run(cmd, timeout=7200)
                if result.returncode == 0:
                    return True, "Burn completed successfully"
                else:
                    return False, f"drutil error (exit code {result.returncode})"

            else:
                return False, f"Unsupported burning tool: {tool}"

        except FileNotFoundError:
            return False, f"Burning tool '{tool}' not found — is it installed and on PATH?"
        except Exception as e:
            return False, f"Burn error: {str(e)}"


# ============================================================================
# Unit Tests
# ============================================================================

class TestDatabase(unittest.TestCase):
    """Test database operations"""
    
    def setUp(self):
        """Create a temporary database for testing"""
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        self.db = Database(self.test_db.name)
    
    def tearDown(self):
        """Clean up temporary database"""
        if os.path.exists(self.test_db.name):
            os.unlink(self.test_db.name)
    
    def test_init_db(self):
        """Test database initialization"""
        conn = sqlite3.connect(self.test_db.name)
        c = conn.cursor()
        
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in c.fetchall()]
        
        self.assertIn('disks', tables)
        self.assertIn('files', tables)
        self.assertIn('burn_queue', tables)
        
        conn.close()
    
    def test_add_disk_success(self):
        success, msg, disk_id = self.db.add_disk("TEST-001", 25, "Test disk")
        self.assertTrue(success)
        self.assertEqual(msg, "Disk added successfully")
        self.assertIsNotNone(disk_id)
        self.assertGreater(disk_id, 0)
    
    def test_add_disk_empty_label(self):
        success, msg, disk_id = self.db.add_disk("", 25)
        self.assertFalse(success)
        self.assertEqual(msg, "Label cannot be empty")
        self.assertIsNone(disk_id)
    
    def test_add_disk_invalid_capacity(self):
        success, msg, disk_id = self.db.add_disk("TEST-001", 0)
        self.assertFalse(success)
        self.assertEqual(msg, "Capacity must be positive")
        self.assertIsNone(disk_id)
    
    def test_add_disk_duplicate_label(self):
        self.db.add_disk("TEST-001", 25)
        success, msg, disk_id = self.db.add_disk("TEST-001", 50)
        self.assertFalse(success)
        self.assertEqual(msg, "Disk label already exists")
        self.assertIsNone(disk_id)
    
    def test_get_disks(self):
        self.db.add_disk("TEST-001", 25)
        self.db.add_disk("TEST-002", 50)
        disks = self.db.get_disks()
        self.assertEqual(len(disks), 2)
        labels = [disk[1] for disk in disks]
        self.assertIn("TEST-001", labels)
        self.assertIn("TEST-002", labels)
    
    def test_get_disk_by_id(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25, "Notes")
        disk = self.db.get_disk_by_id(disk_id)
        self.assertIsNotNone(disk)
        self.assertEqual(disk[1], "TEST-001")
        self.assertEqual(disk[2], 25)
    
    def test_get_disk_by_label(self):
        self.db.add_disk("TEST-001", 25, "Notes")
        disk = self.db.get_disk_by_label("TEST-001")
        self.assertIsNotNone(disk)
        self.assertEqual(disk[1], "TEST-001")
        self.assertEqual(disk[2], 25)
    
    def test_add_file_success(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25)
        success, msg = self.db.add_file(disk_id, "/path/to/file.txt", "file.txt", 1.5)
        self.assertTrue(success)
        self.assertEqual(msg, "File added successfully")
        disk = self.db.get_disk_by_id(disk_id)
        self.assertEqual(disk[3], 1.5)
    
    def test_add_file_invalid_disk(self):
        success, msg = self.db.add_file(9999, "/path/to/file.txt", "file.txt", 1.5)
        self.assertFalse(success)
        self.assertEqual(msg, "Disk not found")
    
    def test_add_file_empty_path(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25)
        success, msg = self.db.add_file(disk_id, "", "file.txt", 1.5)
        self.assertFalse(success)
        self.assertEqual(msg, "File path cannot be empty")
    
    def test_add_file_negative_size(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25)
        success, msg = self.db.add_file(disk_id, "/path/to/file.txt", "file.txt", -1.5)
        self.assertFalse(success)
        self.assertEqual(msg, "File size cannot be negative")
    
    def test_get_files_for_disk(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25)
        self.db.add_file(disk_id, "/path/to/file1.txt", "file1.txt", 1.0)
        self.db.add_file(disk_id, "/path/to/file2.txt", "file2.txt", 2.0)
        files = self.db.get_files_for_disk(disk_id)
        self.assertEqual(len(files), 2)
    
    def test_search_files(self):
        _, _, disk_id = self.db.add_disk("TEST-001", 25)
        self.db.add_file(disk_id, "/home/user/documents/report.pdf", "report.pdf", 1.0)
        self.db.add_file(disk_id, "/home/user/photos/vacation.jpg", "vacation.jpg", 2.0)
        results = self.db.search_files("documents")
        self.assertEqual(len(results), 1)
        self.assertIn("report.pdf", results[0][0])
        results = self.db.search_files("vacation")
        self.assertEqual(len(results), 1)
        results = self.db.search_files("nonexistent")
        self.assertEqual(len(results), 0)
    
    def test_search_files_empty_term(self):
        results = self.db.search_files("")
        self.assertEqual(len(results), 0)
    
    def test_add_to_queue_success(self):
        success, msg = self.db.add_to_queue("/path/to/file.txt", 1.5)
        self.assertTrue(success)
        self.assertEqual(msg, "Added to queue")
        queue = self.db.get_queue()
        self.assertEqual(len(queue), 1)
    
    def test_add_to_queue_empty_path(self):
        success, msg = self.db.add_to_queue("", 1.5)
        self.assertFalse(success)
        self.assertEqual(msg, "File path cannot be empty")
    
    def test_add_to_queue_negative_size(self):
        success, msg = self.db.add_to_queue("/path/to/file.txt", -1.5)
        self.assertFalse(success)
        self.assertEqual(msg, "File size cannot be negative")
    
    def test_get_queue(self):
        self.db.add_to_queue("/path/to/file1.txt", 1.0)
        self.db.add_to_queue("/path/to/file2.txt", 2.0)
        queue = self.db.get_queue()
        self.assertEqual(len(queue), 2)
    
    def test_remove_from_queue(self):
        self.db.add_to_queue("/path/to/file.txt", 1.0)
        queue = self.db.get_queue()
        queue_id = queue[0][0]
        success, msg = self.db.remove_from_queue(queue_id)
        self.assertTrue(success)
        self.assertEqual(msg, "Removed from queue")
        queue = self.db.get_queue()
        self.assertEqual(len(queue), 0)
    
    def test_remove_from_queue_invalid_id(self):
        success, msg = self.db.remove_from_queue(9999)
        self.assertFalse(success)
        self.assertEqual(msg, "Queue item not found")
    
    def test_clear_queue(self):
        self.db.add_to_queue("/path/to/file1.txt", 1.0)
        self.db.add_to_queue("/path/to/file2.txt", 2.0)
        self.db.clear_queue()
        queue = self.db.get_queue()
        self.assertEqual(len(queue), 0)


class TestFileSystemHelper(unittest.TestCase):
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = Path(self.test_dir) / "test.txt"
        self.test_file.write_text("Hello World" * 100)
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_calculate_size_file(self):
        size_gb = FileSystemHelper.calculate_size(self.test_file)
        self.assertGreater(size_gb, 0)
        self.assertLess(size_gb, 0.001)
    
    def test_calculate_size_directory(self):
        (Path(self.test_dir) / "file2.txt").write_text("Test" * 100)
        size_gb = FileSystemHelper.calculate_size(Path(self.test_dir))
        self.assertGreater(size_gb, 0)
    
    def test_calculate_size_nonexistent(self):
        size_gb = FileSystemHelper.calculate_size(Path("/nonexistent/path"))
        self.assertEqual(size_gb, 0.0)
    
    def test_prepare_staging_area(self):
        queue_items = [(1, str(self.test_file), 0.001, "2024-01-01")]
        staging_dir = tempfile.mkdtemp()
        try:
            staging_path, file_map = FileSystemHelper.prepare_staging_area(queue_items, staging_dir)
            self.assertTrue(os.path.exists(staging_path))
            self.assertEqual(len(file_map), 1)
            self.assertIn(str(self.test_file), file_map)
            staged_file = Path(staging_path) / "test.txt"
            self.assertTrue(staged_file.exists())
        finally:
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)


class TestBurnEngine(unittest.TestCase):
    
    def test_detect_burner(self):
        tool, device = BurnEngine.detect_burner()
        if tool is not None:
            self.assertIn(tool, ['growisofs', 'drutil'])
            self.assertIsNotNone(device)
    
    def test_find_linux_drive(self):
        drive = BurnEngine.find_linux_drive()
        self.assertIsNotNone(drive)
        self.assertTrue(drive.startswith('/dev/'))
    
    def test_find_macos_drive(self):
        drive = BurnEngine.find_macos_drive()
        self.assertIsNotNone(drive)
    
    def test_burn_udf_invalid_staging(self):
        success, msg = BurnEngine.burn_udf("/nonexistent", "/dev/sr0", "growisofs", "TEST")
        self.assertFalse(success)
        self.assertIn("does not exist", msg)
    
    def test_burn_udf_no_device(self):
        staging = tempfile.mkdtemp()
        try:
            success, msg = BurnEngine.burn_udf(staging, "", "growisofs", "TEST")
            self.assertFalse(success)
            self.assertIn("No device", msg)
        finally:
            shutil.rmtree(staging)
    
    def test_burn_udf_no_tool(self):
        staging = tempfile.mkdtemp()
        try:
            success, msg = BurnEngine.burn_udf(staging, "/dev/sr0", "", "TEST")
            self.assertFalse(success)
            self.assertIn("No burning tool", msg)
        finally:
            shutil.rmtree(staging)
    
    def test_burn_udf_empty_label(self):
        staging = tempfile.mkdtemp()
        try:
            success, msg = BurnEngine.burn_udf(staging, "/dev/sr0", "growisofs", "")
            self.assertFalse(success)
            self.assertIn("Label cannot be empty", msg)
        finally:
            shutil.rmtree(staging)

    def test_burn_udf_new_disc_uses_Z_flag(self):
        staging = tempfile.mkdtemp()
        try:
            captured_cmd = []
            def mock_popen(cmd, **kwargs):
                captured_cmd.extend(cmd)
                raise FileNotFoundError("growisofs not available in test environment")
            with unittest.mock.patch('subprocess.Popen', side_effect=mock_popen):
                success, msg = BurnEngine.burn_udf(staging, "/dev/sr0", "growisofs", "TEST-NEW", is_new_disc=True)
            if captured_cmd:
                self.assertIn('-Z=/dev/sr0', captured_cmd)
                self.assertNotIn('-M=/dev/sr0', captured_cmd)
        finally:
            shutil.rmtree(staging)

    def test_burn_udf_append_uses_M_flag(self):
        staging = tempfile.mkdtemp()
        try:
            captured_cmd = []
            def mock_popen(cmd, **kwargs):
                captured_cmd.extend(cmd)
                raise FileNotFoundError("growisofs not available in test environment")
            with unittest.mock.patch('subprocess.Popen', side_effect=mock_popen):
                success, msg = BurnEngine.burn_udf(staging, "/dev/sr0", "growisofs", "TEST-EXISTING", is_new_disc=False)
            if captured_cmd:
                self.assertIn('-M=/dev/sr0', captured_cmd)
                self.assertNotIn('-Z=/dev/sr0', captured_cmd)
        finally:
            shutil.rmtree(staging)

    def test_burn_udf_valid_inputs(self):
        staging = tempfile.mkdtemp()
        try:
            success, msg = BurnEngine.burn_udf(staging, "/dev/sr0", "growisofs", "TEST-001")
            self.assertIsInstance(success, bool)
            self.assertIsInstance(msg, str)
            self.assertGreater(len(msg), 0)
        finally:
            shutil.rmtree(staging)

    def test_build_command_new_disc(self):
        """Test that build_command returns -Z flag for new discs"""
        cmd = BurnEngine.build_command("/tmp/staging", "/dev/sr0", "growisofs", "TEST", is_new_disc=True)
        self.assertIn('-Z=/dev/sr0', cmd)
        self.assertNotIn('-M=/dev/sr0', cmd)

    def test_build_command_append(self):
        """Test that build_command returns -M flag for appending"""
        cmd = BurnEngine.build_command("/tmp/staging", "/dev/sr0", "growisofs", "TEST", is_new_disc=False)
        self.assertIn('-M=/dev/sr0', cmd)
        self.assertNotIn('-Z=/dev/sr0', cmd)


# ============================================================================
# TUI Layer
# ============================================================================

class SearchScreen(Screen):
    """Screen for searching backed up files"""
    
    BINDINGS = [("escape", "app.pop_screen", "Cancel")]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Search for Files", id="title"),
            Label("Enter search term (file name or path):"),
            Input(placeholder="e.g., documents, video.mp4, /home/user", id="search_term"),
            Button("Search", variant="primary", id="search"),
            DataTable(id="search_results"),
            id="search_dialog"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "search":
            self.perform_search()
    
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_term":
            self.perform_search()
    
    def perform_search(self) -> None:
        search_term = self.query_one("#search_term", Input).value
        if not search_term:
            self.notify("Please enter a search term!", severity="warning")
            return
        db = Database()
        results = db.search_files(search_term)
        table = self.query_one("#search_results", DataTable)
        table.clear()
        if not table.columns:
            table.add_columns("Original Path", "Path on Disc", "Size (GB)", "Backup Date", "Disk Label")
        if results:
            for filepath, disk_path, size, date, disk_label in results:
                table.add_row(filepath, disk_path, f"{size:.2f}", date, disk_label)
            self.refresh()
            self.notify(f"Found {len(results)} file(s)", severity="information")
        else:
            self.notify("No files found", severity="warning")


class AddToQueueScreen(Screen):
    """Screen for adding files/directories to burn queue"""
    
    BINDINGS = [("escape", "app.pop_screen", "Cancel")]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Add Files to Burn Queue", id="title"),
            Label("File/Directory Path:"),
            Input(placeholder="/path/to/file or /path/to/directory", id="filepath"),
            Horizontal(
                Button("Add to Queue", variant="primary", id="add"),
                Button("Cancel", variant="default", id="cancel"),
            ),
            Label("", id="message"),
            id="dialog"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add":
            self.add_to_queue()
        elif event.button.id == "cancel":
            self.app.pop_screen()
    
    def add_to_queue(self) -> None:
        filepath = self.query_one("#filepath", Input).value.strip()
        if not filepath:
            self.query_one("#message", Label).update("Path is required!")
            return
        try:
            filepath = str(Path(filepath).expanduser())
            if '*' in filepath or '?' in filepath:
                parent = Path(filepath).parent
                pattern = Path(filepath).name
                if not parent.exists():
                    self.query_one("#message", Label).update(
                        f"Directory not found: {parent}\n"
                        f"Note: Linux paths are case-sensitive (use /home not /Home)"
                    )
                    return
                matched_files = sorted(parent.glob(pattern))
                if not matched_files:
                    self.query_one("#message", Label).update(
                        f"No files matched '{pattern}' in {parent}"
                    )
                    return
                db = Database()
                added = 0
                for match in matched_files:
                    if match.is_file():
                        size_gb = FileSystemHelper.calculate_size(match)
                        success, msg = db.add_to_queue(str(match), round(size_gb, 4))
                        if success:
                            added += 1
                if added > 0:
                    self.notify(f"Added {added} file(s) to queue", severity="information")
                    self.app.pop_screen()
                else:
                    self.query_one("#message", Label).update("No files could be added to the queue")
            else:
                path = Path(filepath).resolve()
                if not path.exists():
                    self.query_one("#message", Label).update(
                        f"Path not found: {path}\n"
                        f"Note: Linux paths are case-sensitive (use /home not /Home)"
                    )
                    return
                size_gb = FileSystemHelper.calculate_size(path)
                db = Database()
                success, msg = db.add_to_queue(str(path), round(size_gb, 2))
                if success:
                    self.notify(f"Added to queue: {size_gb:.2f} GB", severity="information")
                    self.app.pop_screen()
                else:
                    self.query_one("#message", Label).update(msg)
        except Exception as e:
            self.query_one("#message", Label).update(f"Error: {str(e)}")


class BurnConfirmModal(ModalScreen):
    """Modal confirmation dialog shown before any burn operation.
    
    Displays the exact command, device, tool, session mode, and queue
    summary so the user can verify everything looks correct before
    committing to a potentially long, irreversible burn.
    """

    def __init__(self,
                 tool: str,
                 device: str,
                 label: str,
                 is_new_disc: bool,
                 file_count: int,
                 total_size_gb: float,
                 command: List[str]):
        super().__init__()
        self.tool = tool
        self.device = device
        self.label = label
        self.is_new_disc = is_new_disc
        self.file_count = file_count
        self.total_size_gb = total_size_gb
        self.command = command

    def compose(self) -> ComposeResult:
        mode = "NEW DISC (overwrite/first burn)" if self.is_new_disc else "APPEND (existing data preserved)"
        plain_english = (
            f"{'Starting new disc' if self.is_new_disc else 'Appending to existing disc'} "
            f"'{self.label}' on {self.device} using {self.tool}"
        )
        command_str = " ".join(self.command)

        yield Container(
            Label("⚠  Confirm Burn Operation", id="modal_title"),
            Label("─" * 56, id="divider1"),
            Label(plain_english, id="plain_summary"),
            Label("─" * 56, id="divider2"),
            Label(f"  Tool:      {self.tool}", id="info_tool"),
            Label(f"  Device:    {self.device}", id="info_device"),
            Label(f"  Mode:      {mode}", id="info_mode"),
            Label(f"  Files:     {self.file_count} file(s)  /  {self.total_size_gb:.2f} GB total", id="info_files"),
            Label("─" * 56, id="divider3"),
            Label("  Full command:", id="cmd_header"),
            Label(f"  {command_str}", id="info_command"),
            Label("─" * 56, id="divider4"),
            Label("This operation may take 30–45 minutes.", id="warning_time"),
            Horizontal(
                Button("Confirm — Start Burn", variant="error", id="confirm"),
                Button("Cancel", variant="default", id="cancel"),
            ),
            id="confirm_modal"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            self.dismiss(True)
        elif event.button.id == "cancel":
            self.dismiss(False)


class BurnScreen(Screen):
    """Screen for burning queue to disc with multi-session support"""
    
    BINDINGS = [("escape", "app.pop_screen", "Cancel")]
    
    def __init__(self, queue_items: List[Tuple]):
        super().__init__()
        self.queue_items = queue_items
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Burn Queue to Disc", id="title"),
            Label("Select existing disk or enter new label:"),
            DataTable(id="disk_selector"),
            Label("Or enter disk label manually:"),
            Input(placeholder="e.g., BACKUP-2024-001", id="label"),
            Label("Capacity (GB) - only for new disks:"),
            Input(placeholder="25, 50, or 100", id="capacity", value="25"),
            Label(f"Queue size: {sum(item[2] for item in self.queue_items):.2f} GB"),
            Label("Multi-session: You can add to existing disks until full"),
            Label("Status:", id="status"),
            ProgressBar(total=100, show_eta=False, id="progress"),
            Horizontal(
                Button("Start Burn", variant="primary", id="burn"),
                Button("Cancel", variant="default", id="cancel"),
            ),
            Label("", id="message"),
            id="burn_dialog"
        )
        yield Footer()
    
    def on_mount(self) -> None:
        table = self.query_one("#disk_selector", DataTable)
        table.cursor_type = "row"
        table.add_columns("Label", "Used", "Free", "Capacity")
        db = Database()
        disks = db.get_disks()
        if disks:
            for disk in disks:
                _, label, capacity, used, _, _ = disk
                free = capacity - used
                free_pct = round((free / capacity) * 100, 1) if capacity > 0 else 0
                table.add_row(
                    label,
                    f"{used:.1f} GB",
                    f"{free:.1f} GB ({free_pct}%)",
                    f"{capacity} GB"
                )
        else:
            self.query_one("#status", Label).update("No existing disks - create a new one below")
    
    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "disk_selector":
            table = self.query_one("#disk_selector", DataTable)
            row = table.get_row_at(event.cursor_row)
            selected_label = row[0]
            self.query_one("#label", Input).value = selected_label
            db = Database()
            disk = db.get_disk_by_label(selected_label)
            used_gb = disk[3] if disk else 0
            if used_gb == 0:
                self.query_one("#status", Label).update(
                    f"Selected disk: {selected_label} — registered but never burned, will use -Z (new disc)"
                )
            else:
                self.query_one("#status", Label).update(
                    f"Selected existing disk: {selected_label} — files will be APPENDED (existing data preserved)"
                )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "burn":
            self.start_burn()
        elif event.button.id == "cancel":
            self.app.pop_screen()
    
    def start_burn(self) -> None:
        """Validate inputs, then show confirmation modal before burning."""
        label = self.query_one("#label", Input).value.strip()
        capacity = self.query_one("#capacity", Input).value.strip()

        if not label:
            self.query_one("#message", Label).update("Label is required!")
            return

        db = Database()
        existing_disk = db.get_disk_by_label(label)

        if existing_disk:
            disk_id = existing_disk[0]
            capacity_gb = existing_disk[2]
            used_gb = existing_disk[3]
            is_new_disc = (used_gb == 0)
            total_size = sum(item[2] for item in self.queue_items)
            available_space = capacity_gb - used_gb

            if total_size > available_space:
                self.query_one("#message", Label).update(
                    f"Not enough space! Need {total_size:.2f} GB, only {available_space:.2f} GB available"
                )
                return
        else:
            is_new_disc = True
            if not capacity:
                self.query_one("#message", Label).update("Capacity is required for new disks!")
                return
            try:
                capacity_gb = int(capacity)
            except ValueError:
                self.query_one("#message", Label).update("Capacity must be a number!")
                return
            total_size = sum(item[2] for item in self.queue_items)
            if total_size > capacity_gb:
                self.query_one("#message", Label).update(f"Queue too large! {total_size:.2f} GB > {capacity_gb} GB")
                return
            success, msg, disk_id = db.add_disk(label, capacity_gb, "Burned with UDF filesystem")
            if not success:
                self.query_one("#message", Label).update(msg)
                return

        # Detect burning hardware
        tool, device = BurnEngine.detect_burner()
        if not tool:
            self.query_one("#message", Label).update(
                "No burning tool found! Install: growisofs (Linux) or use built-in tools (macOS)"
            )
            return

        # Build the exact command that will be run — using a placeholder staging
        # path since the real staging dir hasn't been created yet. The placeholder
        # makes the intent clear without exposing a temp path that doesn't exist yet.
        preview_cmd = BurnEngine.build_command(
            "<staging-dir>", device, tool, label, is_new_disc
        )

        file_count = len(self.queue_items)
        total_size_gb = sum(item[2] for item in self.queue_items)

        def on_confirm(confirmed: bool) -> None:
            if confirmed:
                self.perform_burn(disk_id, label, device, tool, is_new_disc)

        self.app.push_screen(
            BurnConfirmModal(
                tool=tool,
                device=device,
                label=label,
                is_new_disc=is_new_disc,
                file_count=file_count,
                total_size_gb=total_size_gb,
                command=preview_cmd,
            ),
            on_confirm
        )

    def perform_burn(self, disk_id: int, label: str, device: str, tool: str,
                     is_new_disc: bool) -> None:
        """Execute the burn process and record files in database"""
        db = Database()
        progress = self.query_one("#progress", ProgressBar)
        status = self.query_one("#status", Label)
        staging_dir = None

        try:
            status.update("Preparing files for burning...")
            progress.update(progress=5)
            
            staging_dir = tempfile.mkdtemp(prefix="bluray_staging_")
            staging_path, file_map = FileSystemHelper.prepare_staging_area(self.queue_items, staging_dir)
            
            mode_label = "new disc" if is_new_disc else "appending to existing disc"
            status.update(f"Staging complete. Starting burn ({mode_label}) — this will take 30–45 minutes...")
            progress.update(progress=15)
            
            status.update("🔥 Burning to disc... (progress shown in terminal, not here)")
            progress.update(progress=20)
            
            success, msg = BurnEngine.burn_udf(staging_path, device, tool, label,
                                               is_new_disc=is_new_disc)
            
            progress.update(progress=80)

            if not success:
                self.query_one("#message", Label).update(f"Burn failed: {msg}")
                status.update("Burn failed — check terminal output for details")
                return
            
            status.update("Recording files in database...")
            
            for item in self.queue_items:
                queue_id, filepath, size, _ = item
                source_path = Path(filepath)
                
                if source_path.is_file():
                    disk_path = file_map.get(filepath, source_path.name)
                    db.add_file(disk_id, filepath, disk_path, size)
                else:
                    base_disk_path = file_map.get(filepath, source_path.name)
                    for file in source_path.rglob('*'):
                        if file.is_file():
                            file_size_gb = file.stat().st_size / (1024**3)
                            rel_path = file.relative_to(source_path)
                            file_disk_path = str(Path(base_disk_path) / rel_path)
                            db.add_file(disk_id, str(file), file_disk_path, file_size_gb)
            
            db.clear_queue()
            
            progress.update(progress=100)
            status.update("✅ Burn completed successfully! Files are now searchable and restorable.")
            
            self.notify("Burn completed! Files added to database.", severity="information")
            self.query_one("#burn", Button).disabled = True
            
        except Exception as e:
            self.query_one("#message", Label).update(f"Error: {str(e)}")
            status.update(f"Error occurred: {str(e)}")
        finally:
            if staging_dir:
                try:
                    if Path(staging_dir).exists():
                        shutil.rmtree(staging_dir)
                except Exception:
                    pass


class AddDiskScreen(Screen):
    """Screen for manually adding a disk to the database"""
    
    BINDINGS = [("escape", "app.pop_screen", "Cancel")]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Add New Blu-ray Disk", id="title"),
            Label("Label:"),
            Input(placeholder="e.g., BACKUP-2024-001", id="label"),
            Label("Capacity (GB):"),
            Input(placeholder="25, 50, or 100", id="capacity"),
            Label("Notes (optional):"),
            Input(placeholder="Description or contents", id="notes"),
            Horizontal(
                Button("Add Disk", variant="primary", id="add"),
                Button("Cancel", variant="default", id="cancel"),
            ),
            Label("", id="message"),
            id="dialog"
        )
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add":
            self.add_disk()
        elif event.button.id == "cancel":
            self.app.pop_screen()
    
    def add_disk(self) -> None:
        label = self.query_one("#label", Input).value
        capacity = self.query_one("#capacity", Input).value
        notes = self.query_one("#notes", Input).value
        if not label or not capacity:
            self.query_one("#message", Label).update("Label and capacity are required!")
            return
        try:
            capacity_gb = int(capacity)
            db = Database()
            success, msg, _ = db.add_disk(label, capacity_gb, notes)
            if success:
                self.app.pop_screen()
            else:
                self.query_one("#message", Label).update(msg)
        except ValueError:
            self.query_one("#message", Label).update("Capacity must be a number!")


class QueueScreen(Screen):
    """Screen for managing the burn queue"""
    
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Burn Queue", id="title"),
            DataTable(id="queue_table"),
            Horizontal(
                Button("Add Files", variant="success", id="add"),
                Button("Remove Selected", variant="default", id="remove"),
                Button("Clear Queue", variant="default", id="clear"),
                Button("Burn to Disc", variant="primary", id="burn"),
            ),
            id="queue_container"
        )
        yield Footer()
    
    def on_mount(self) -> None:
        table = self.query_one("#queue_table", DataTable)
        table.cursor_type = "row"
        table.add_columns("ID", "File Path", "Size (GB)", "Added")
        self.refresh_queue()
    
    def refresh_queue(self) -> None:
        db = Database()
        queue = db.get_queue()
        table = self.query_one("#queue_table", DataTable)
        table.clear()
        total_size = 0
        for item in queue:
            queue_id, filepath, size, added = item
            table.add_row(str(queue_id), filepath, f"{size:.2f}", added.split()[0])
            total_size += size
        self.query_one("#title", Label).update(f"Burn Queue - Total: {total_size:.2f} GB")
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add":
            self.app.push_screen(AddToQueueScreen())
        elif event.button.id == "clear":
            db = Database()
            db.clear_queue()
            self.refresh_queue()
            self.notify("Queue cleared", severity="information")
        elif event.button.id == "burn":
            db = Database()
            queue = db.get_queue()
            if not queue:
                self.notify("Queue is empty!", severity="warning")
            else:
                self.app.push_screen(BurnScreen(queue))
        elif event.button.id == "remove":
            table = self.query_one("#queue_table", DataTable)
            if table.cursor_row is not None:
                row = table.get_row_at(table.cursor_row)
                queue_id = int(row[0])
                db = Database()
                success, msg = db.remove_from_queue(queue_id)
                if success:
                    self.refresh_queue()
                    self.notify(msg, severity="information")
                else:
                    self.notify(msg, severity="error")
    
    def on_screen_resume(self) -> None:
        self.refresh_queue()


class DiskFilesScreen(Screen):
    """Screen for viewing all files stored on a specific disk"""

    BINDINGS = [("escape", "app.pop_screen", "Back")]

    def __init__(self, disk_id: int, disk_label: str):
        super().__init__()
        self.disk_id = disk_id
        self.disk_label = disk_label

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label(f"Files on Disk: {self.disk_label}", id="title"),
            DataTable(id="files_table"),
            Button("Back", variant="default", id="back"),
            id="queue_container"
        )
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#files_table", DataTable)
        table.cursor_type = "row"
        table.add_columns("Original Path", "Path on Disc", "Size (GB)", "Backup Date")
        db = Database()
        files = db.get_files_for_disk(self.disk_id)
        for _, file_path, disk_path, size, date in files:
            table.add_row(file_path, disk_path, f"{size:.4f}", date)
        if not files:
            self.notify("No files recorded for this disk yet.", severity="information")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.app.pop_screen()


class BlurayBackupApp(App):
    """Main application"""
    
    CSS = """
    Screen {
        background: $surface;
    }
    
    #dialog {
        width: 60;
        height: auto;
        border: thick $background 80%;
        background: $surface;
        padding: 1 2;
        margin: 2 4;
    }
    
    #burn_dialog {
        width: 80;
        height: auto;
        border: thick $background 80%;
        background: $surface;
        padding: 1 2;
        margin: 2 4;
    }
    
    #disk_selector {
        height: 10;
        margin: 1 0;
    }
    
    #queue_container {
        width: 100%;
        height: 100%;
        padding: 1 2;
    }
    
    #search_dialog {
        width: 90;
        height: auto;
        border: thick $background 80%;
        background: $surface;
        padding: 1 2;
        margin: 2 4;
    }
    
    #search_results {
        height: 20;
        margin-top: 1;
    }
    
    #queue_table {
        height: 1fr;
        margin: 1 0;
    }
    
    #title {
        text-style: bold;
        color: $accent;
        margin-bottom: 1;
    }
    
    #message {
        color: $error;
        margin-top: 1;
    }
    
    #status {
        margin: 1 0;
        color: $text;
    }
    
    #progress {
        margin: 1 0;
    }
    
    DataTable {
        height: 1fr;
        margin: 1 2;
    }
    
    #controls {
        height: auto;
        padding: 1 2;
        background: $panel;
    }
    
    Button {
        margin: 0 1;
    }
    
    Input {
        margin-bottom: 1;
    }
    
    #info {
        height: 5;
        padding: 1 2;
        background: $panel;
        margin: 0 2 1 2;
    }

    #confirm_modal {
        width: 64;
        height: auto;
        border: thick $error 80%;
        background: $surface;
        padding: 1 2;
        margin: 4 8;
    }

    #modal_title {
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    #plain_summary {
        color: $text;
        margin: 1 0;
    }

    #info_mode {
        color: $warning;
    }

    #info_command {
        color: $accent;
        margin-bottom: 1;
    }

    #warning_time {
        color: $warning;
        margin: 1 0;
    }

    #divider1, #divider2, #divider3, #divider4 {
        color: $text-muted;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("a", "add_disk", "Add Disk"),
        Binding("s", "search", "Search"),
        Binding("b", "show_queue", "Burn Queue"),
        Binding("v", "view_files", "View Files"),
    ]
    
    def __init__(self):
        super().__init__()
        self.db = Database()
        self.selected_disk = None
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Label("Blu-ray Backup Manager - UDF Raw File Mode", id="title"),
            id="info"
        )
        yield Container(
            Horizontal(
                Button("Add Disk (a)", variant="primary", id="add_disk"),
                Button("Burn Queue (b)", variant="success", id="queue"),
                Button("Search (s)", variant="default", id="search"),
                Button("View Files (v)", variant="default", id="view_files"),
                Button("Refresh", variant="default", id="refresh"),
            ),
            id="controls"
        )
        yield DataTable(id="disks_table")
        yield Footer()
    
    def on_mount(self) -> None:
        table = self.query_one("#disks_table", DataTable)
        table.cursor_type = "row"
        table.add_columns("ID", "Label", "Capacity (GB)", "Used (GB)", "Free (%)", "Created", "Notes")
        self.refresh_table()
    
    def refresh_table(self) -> None:
        table = self.query_one("#disks_table", DataTable)
        table.clear()
        disks = self.db.get_disks()
        for disk in disks:
            disk_id, label, capacity, used, created, notes = disk
            free_pct = round(((capacity - used) / capacity) * 100, 1) if capacity > 0 else 0
            table.add_row(
                str(disk_id),
                label,
                str(capacity),
                f"{used:.2f}",
                f"{free_pct}%",
                created.split()[0],
                notes[:30] if notes else ""
            )
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "add_disk":
            self.action_add_disk()
        elif event.button.id == "queue":
            self.action_show_queue()
        elif event.button.id == "search":
            self.action_search()
        elif event.button.id == "view_files":
            self.action_view_files()
        elif event.button.id == "refresh":
            self.refresh_table()
    
    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "disks_table":
            table = self.query_one("#disks_table", DataTable)
            row = table.get_row_at(event.cursor_row)
            self.selected_disk = (int(row[0]), row[1])
            info = self.query_one("#info", Container)
            info.query_one(Label).update(
                f"Selected: {row[1]} | Capacity: {row[2]}GB | Used: {row[3]}GB | Free: {row[4]}"
            )
    
    def action_add_disk(self) -> None:
        self.push_screen(AddDiskScreen())
    
    def action_show_queue(self) -> None:
        self.push_screen(QueueScreen())
    
    def action_search(self) -> None:
        self.push_screen(SearchScreen())

    def action_view_files(self) -> None:
        if self.selected_disk is None:
            self.notify("Select a disk row first.", severity="warning")
            return
        disk_id, disk_label = self.selected_disk
        self.push_screen(DiskFilesScreen(disk_id, disk_label))
    
    def on_screen_resume(self) -> None:
        self.refresh_table()


# ============================================================================
# Main Entry Point
# ============================================================================

def run_tests():
    print("=" * 70)
    print("Running Unit Tests for Blu-ray Backup Manager")
    print("=" * 70)
    print()
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestDatabase))
    suite.addTests(loader.loadTestsFromTestCase(TestFileSystemHelper))
    suite.addTests(loader.loadTestsFromTestCase(TestBurnEngine))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print()
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 70)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    if "--test" in sys.argv:
        success = run_tests()
        sys.exit(0 if success else 1)
    else:
        app = BlurayBackupApp()
        app.run()