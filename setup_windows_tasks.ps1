# VIX Data Collection - Windows Task Scheduler Setup
# PowerShell script to create scheduled tasks for daily VIX data collection

param(
    [string]$ProjectPath = "C:\VIXDataCollection",
    [string]$PythonPath = "python",
    [string]$CollectionTime = "18:30",
    [switch]$CreateService,
    [switch]$RemoveTasks
)

# Check if running as Administrator
function Test-Admin {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Error "This script must be run as Administrator. Please run PowerShell as Administrator and try again."
    exit 1
}

Write-Host "🚀 VIX Data Collection - Windows Task Scheduler Setup" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green

# Validate Python installation
Write-Host "📋 Validating Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = & $PythonPath --version 2>&1
    Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Error "❌ Python not found at '$PythonPath'. Please install Python or provide correct path."
    exit 1
}

# Validate project directory
if (-not (Test-Path $ProjectPath)) {
    Write-Host "📁 Creating project directory: $ProjectPath" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $ProjectPath -Force | Out-Null
}

# Define script paths
$VIXFetcherScript = Join-Path $ProjectPath "scripts\vix_data_fetcher.py"
$SchedulerScript = Join-Path $ProjectPath "scripts\vix_daily_scheduler.py"
$LogPath = Join-Path $ProjectPath "logs"
$DataPath = Join-Path $ProjectPath "data\vix_data"

# Create directory structure
Write-Host "📁 Creating directory structure..." -ForegroundColor Yellow
$directories = @($LogPath, $DataPath, (Join-Path $ProjectPath "scripts"), (Join-Path $ProjectPath "config"))
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "   Created: $dir" -ForegroundColor Gray
    }
}

# Remove existing tasks if requested
if ($RemoveTasks) {
    Write-Host "🗑️ Removing existing VIX data collection tasks..." -ForegroundColor Yellow
    
    $tasks = @("VIX-DailyDataCollection", "VIX-WeekendMaintenance", "VIX-SystemMonitor")
    foreach ($taskName in $tasks) {
        try {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
            Write-Host "   Removed: $taskName" -ForegroundColor Gray
        } catch {
            Write-Host "   Task not found: $taskName" -ForegroundColor Gray
        }
    }
    Write-Host "✅ Task removal completed" -ForegroundColor Green
    return
}

# Create wrapper batch file for reliable execution
$BatchFile = Join-Path $ProjectPath "run_vix_collection.bat"
$BatchContent = @"
@echo off
cd /d "$ProjectPath"
echo [%date% %time%] Starting VIX data collection... >> "$LogPath\task_execution.log"

REM Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
    echo [%date% %time%] Virtual environment activated >> "$LogPath\task_execution.log"
)

REM Run the VIX data collection
"$PythonPath" "$VIXFetcherScript" >> "$LogPath\vix_collection.log" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] VIX collection completed successfully >> "$LogPath\task_execution.log"
) else (
    echo [%date% %time%] VIX collection failed with error code %ERRORLEVEL% >> "$LogPath\task_execution.log"
)

echo [%date% %time%] Task execution finished >> "$LogPath\task_execution.log"
"@

Write-Host "📄 Creating execution wrapper..." -ForegroundColor Yellow
$BatchContent | Out-File -FilePath $BatchFile -Encoding ASCII
Write-Host "   Created: $BatchFile" -ForegroundColor Gray

# Create PowerShell monitoring script
$MonitorScript = Join-Path $ProjectPath "monitor_vix_system.ps1"
$MonitorContent = @'
# VIX Data Collection System Monitor
param([switch]$Detailed)

$ProjectPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogPath = Join-Path $ProjectPath "logs"
$DataPath = Join-Path $ProjectPath "data\vix_data"

Write-Host "🔍 VIX Data Collection System Status" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Green
Write-Host "📅 Check Time: $(Get-Date)" -ForegroundColor Gray

# Check scheduled tasks
Write-Host "`n📋 Scheduled Tasks:" -ForegroundColor Yellow
$tasks = @("VIX-DailyDataCollection", "VIX-WeekendMaintenance")
foreach ($task in $tasks) {
    try {
        $taskInfo = Get-ScheduledTask -TaskName $task -ErrorAction Stop
        $lastRun = (Get-ScheduledTaskInfo -TaskName $task).LastRunTime
        $nextRun = (Get-ScheduledTaskInfo -TaskName $task).NextRunTime
        $state = $taskInfo.State
        
        Write-Host "   ✅ $task : $state" -ForegroundColor Green
        if ($Detailed) {
            Write-Host "      Last Run: $lastRun" -ForegroundColor Gray
            Write-Host "      Next Run: $nextRun" -ForegroundColor Gray
        }
    } catch {
        Write-Host "   ❌ $task : Not Found" -ForegroundColor Red
    }
}

# Check recent data files
Write-Host "`n📊 Recent Data Files:" -ForegroundColor Yellow
if (Test-Path $DataPath) {
    $recentFiles = Get-ChildItem $DataPath -Filter "*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 5
    if ($recentFiles) {
        foreach ($file in $recentFiles) {
            $age = (Get-Date) - $file.LastWriteTime
            $ageText = if ($age.TotalDays -gt 1) { "$([math]::Round($age.TotalDays,1)) days ago" } else { "$([math]::Round($age.TotalHours,1)) hours ago" }
            Write-Host "   📄 $($file.Name) ($ageText)" -ForegroundColor Gray
        }
    } else {
        Write-Host "   ⚠️ No CSV files found" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ❌ Data directory not found" -ForegroundColor Red
}

# Check log files
Write-Host "`n📝 Log Files:" -ForegroundColor Yellow
if (Test-Path $LogPath) {
    $logFiles = Get-ChildItem $LogPath -Filter "*.log" | Sort-Object LastWriteTime -Descending
    foreach ($log in $logFiles) {
        $size = [math]::Round($log.Length / 1KB, 2)
        Write-Host "   📝 $($log.Name) (${size} KB)" -ForegroundColor Gray
    }
} else {
    Write-Host "   ❌ Log directory not found" -ForegroundColor Red
}

# Disk space check
Write-Host "`n💾 Disk Space:" -ForegroundColor Yellow
$drive = (Get-Item $ProjectPath).PSDrive
$freeSpace = [math]::Round($drive.Free / 1GB, 2)
$totalSpace = [math]::Round(($drive.Free + $drive.Used) / 1GB, 2)
$usedPercent = [math]::Round((1 - ($drive.Free / ($drive.Free + $drive.Used))) * 100, 1)

Write-Host "   💾 Drive $($drive.Name): ${freeSpace} GB free of ${totalSpace} GB (${usedPercent}% used)" -ForegroundColor Gray

if ($freeSpace -lt 5) {
    Write-Host "   ⚠️ Low disk space warning!" -ForegroundColor Yellow
}

Write-Host "`n✅ System check completed" -ForegroundColor Green
'@

$MonitorContent | Out-File -FilePath $MonitorScript -Encoding UTF8
Write-Host "   Created: $MonitorScript" -ForegroundColor Gray

# Create main daily collection task
Write-Host "⏰ Creating daily VIX data collection task..." -ForegroundColor Yellow

$TaskName = "VIX-DailyDataCollection"
$TaskDescription = "Daily VIX futures and options data collection from Bloomberg"

# Create task action
$Action = New-ScheduledTaskAction -Execute $BatchFile

# Create task trigger (daily at specified time, Monday-Friday)
$Trigger = New-ScheduledTaskTrigger -Daily -At $CollectionTime
$Trigger.DaysOfWeek = [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Monday, 
                      [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Tuesday,
                      [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Wednesday,
                      [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Thursday,
                      [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Friday

# Create task settings
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable -StartWhenAvailable

# Create task principal (run as current user)
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

# Register the task
try {
    Register-ScheduledTask -TaskName $TaskName -Description $TaskDescription -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Force
    Write-Host "✅ Task '$TaskName' created successfully" -ForegroundColor Green
    Write-Host "   Schedule: Daily at $CollectionTime (Monday-Friday)" -ForegroundColor Gray
} catch {
    Write-Error "❌ Failed to create task '$TaskName': $($_.Exception.Message)"
}

# Create weekend maintenance task
Write-Host "⏰ Creating weekend maintenance task..." -ForegroundColor Yellow

$MaintenanceTaskName = "VIX-WeekendMaintenance"
$MaintenanceAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-File `"$MonitorScript`" -Detailed"
$MaintenanceTrigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Saturday -At "10:00"

try {
    Register-ScheduledTask -TaskName $MaintenanceTaskName -Description "Weekly VIX data system maintenance and health check" -Action $MaintenanceAction -Trigger $MaintenanceTrigger -Settings $Settings -Principal $Principal -Force
    Write-Host "✅ Task '$MaintenanceTaskName' created successfully" -ForegroundColor Green
    Write-Host "   Schedule: Weekly on Saturday at 10:00 AM" -ForegroundColor Gray
} catch {
    Write-Error "❌ Failed to create task '$MaintenanceTaskName': $($_.Exception.Message)"
}

# Create system monitoring task (optional)
if ($CreateService) {
    Write-Host "⏰ Creating system monitoring task..." -ForegroundColor Yellow
    
    $MonitorTaskName = "VIX-SystemMonitor"
    $MonitorAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-File `"$MonitorScript`""
    $MonitorTrigger = New-ScheduledTaskTrigger -Daily -At "12:00"
    
    try {
        Register-ScheduledTask -TaskName $MonitorTaskName -Description "Daily VIX data system health monitoring" -Action $MonitorAction -Trigger $MonitorTrigger -Settings $Settings -Principal $Principal -Force
        Write-Host "✅ Task '$MonitorTaskName' created successfully" -ForegroundColor Green
        Write-Host "   Schedule: Daily at 12:00 PM" -ForegroundColor Gray
    } catch {
        Write-Error "❌ Failed to create task '$MonitorTaskName': $($_.Exception.Message)"
    }
}

# Create installation summary
Write-Host "`n📋 Installation Summary:" -ForegroundColor Green
Write-Host "========================" -ForegroundColor Green
Write-Host "📁 Project Directory: $ProjectPath" -ForegroundColor Gray
Write-Host "🐍 Python Path: $PythonPath" -ForegroundColor Gray
Write-Host "⏰ Collection Time: $CollectionTime (Monday-Friday)" -ForegroundColor Gray
Write-Host "📄 Execution Script: $BatchFile" -ForegroundColor Gray
Write-Host "📊 Monitor Script: $MonitorScript" -ForegroundColor Gray

Write-Host "`n📝 Next Steps:" -ForegroundColor Yellow
Write-Host "1. Copy your VIX data fetcher scripts to: $ProjectPath\scripts\" -ForegroundColor Gray
Write-Host "2. Configure email/Slack notifications in: $ProjectPath\config\" -ForegroundColor Gray
Write-Host "3. Test the system: & '$MonitorScript'" -ForegroundColor Gray
Write-Host "4. Verify Bloomberg Terminal is accessible" -ForegroundColor Gray

Write-Host "`n🎮 Management Commands:" -ForegroundColor Yellow
Write-Host "• View tasks: Get-ScheduledTask -TaskName 'VIX-*'" -ForegroundColor Gray
Write-Host "• Run monitor: & '$MonitorScript'" -ForegroundColor Gray
Write-Host "• Check logs: Get-Content '$LogPath\*.log'" -ForegroundColor Gray
Write-Host "• Remove tasks: .\setup_windows_tasks.ps1 -RemoveTasks" -ForegroundColor Gray

Write-Host "`n✅ Windows Task Scheduler setup completed!" -ForegroundColor Green

# Test the monitoring script
Write-Host "`n🔍 Running initial system check..." -ForegroundColor Yellow
& $MonitorScript

Write-Host "`n🎊 Setup completed successfully! Your VIX data collection system is ready." -ForegroundColor Green
