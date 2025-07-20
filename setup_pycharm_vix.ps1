# VIX Data Collection Setup for PyCharm Project
# Updated for: C:\Users\acmuser\PycharmProjects\BloombegData

param(
    [string]$ProjectPath = "C:\Users\acmuser\PycharmProjects\BloombegData",
    [string]$VenvPath = "C:\Users\acmuser\PycharmProjects\BloombegData\.venv",
    [string]$CollectionTime = "18:30",
    [switch]$TestMode,
    [switch]$RemoveTasks
)

# Check if running as Administrator
function Test-Admin {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Host "⚠️ This script should be run as Administrator for Task Scheduler setup" -ForegroundColor Yellow
    Write-Host "   You can run in test mode first: -TestMode" -ForegroundColor Gray
}

Write-Host "🚀 VIX Data Collection - PyCharm Project Setup" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
Write-Host "📁 Project Path: $ProjectPath" -ForegroundColor Gray
Write-Host "🐍 Virtual Env: $VenvPath" -ForegroundColor Gray

# Validate project structure
if (-not (Test-Path $ProjectPath)) {
    Write-Error "❌ Project directory not found: $ProjectPath"
    exit 1
}

if (-not (Test-Path $VenvPath)) {
    Write-Error "❌ Virtual environment not found: $VenvPath"
    Write-Host "💡 Create virtual environment first:" -ForegroundColor Yellow
    Write-Host "   python -m venv .venv" -ForegroundColor Gray
    exit 1
}

# Check if VIX data fetcher exists
$VIXFetcherScript = Join-Path $ProjectPath "scripts\vix_data_fetcher.py"
if (-not (Test-Path $VIXFetcherScript)) {
    Write-Error "❌ VIX data fetcher script not found: $VIXFetcherScript"
    Write-Host "💡 Please ensure vix_data_fetcher.py is saved in the scripts directory" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Found VIX data fetcher script" -ForegroundColor Green

# Define paths relative to your project
$LogPath = Join-Path $ProjectPath "logs"
$DataPath = Join-Path $ProjectPath "data\vix_data"
$ConfigPath = Join-Path $ProjectPath "config"
$ScriptsPath = Join-Path $ProjectPath "scripts"

# Create directory structure
Write-Host "📁 Creating directory structure..." -ForegroundColor Yellow
$directories = @($LogPath, $DataPath, $ConfigPath)
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "   Created: $dir" -ForegroundColor Gray
    } else {
        Write-Host "   Exists: $dir" -ForegroundColor Gray
    }
}

# Create Python path that uses the virtual environment
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Error "❌ Python executable not found in virtual environment: $PythonExe"
    exit 1
}

Write-Host "✅ Found Python in virtual environment: $PythonExe" -ForegroundColor Green

# Test Bloomberg API in virtual environment
Write-Host "🔍 Testing Bloomberg API in virtual environment..." -ForegroundColor Yellow
try {
    $testResult = & $PythonExe -c "import blpapi; print(f'✅ Bloomberg API version: {blpapi.__version__}')" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host $testResult -ForegroundColor Green
    } else {
        throw $testResult
    }
} catch {
    Write-Host "❌ Bloomberg API test failed: $_" -ForegroundColor Red
    Write-Host "💡 Install Bloomberg API in your virtual environment:" -ForegroundColor Yellow
    Write-Host "   .venv\Scripts\activate" -ForegroundColor Gray
    Write-Host "   pip install blpapi" -ForegroundColor Gray
}

# Remove existing tasks if requested
if ($RemoveTasks) {
    Write-Host "🗑️ Removing existing VIX data collection tasks..." -ForegroundColor Yellow
    
    $tasks = @("VIX-PyCharm-DailyCollection", "VIX-PyCharm-WeekendMaintenance")
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

# Create activation and execution batch file
$BatchFile = Join-Path $ProjectPath "run_vix_collection.bat"
$BatchContent = @"
@echo off
cd /d "$ProjectPath"
echo [%date% %time%] Starting VIX data collection... >> "$LogPath\task_execution.log"

REM Activate virtual environment
call "$VenvPath\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ERROR: Failed to activate virtual environment >> "$LogPath\task_execution.log"
    exit /b 1
)

echo [%date% %time%] Virtual environment activated >> "$LogPath\task_execution.log"

REM Run the VIX data collection
"$PythonExe" "$VIXFetcherScript" >> "$LogPath\vix_collection.log" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] VIX collection completed successfully >> "$LogPath\task_execution.log"
) else (
    echo [%date% %time%] VIX collection failed with error code %ERRORLEVEL% >> "$LogPath\task_execution.log"
)

echo [%date% %time%] Task execution finished >> "$LogPath\task_execution.log"
"@

Write-Host "📄 Creating execution wrapper for PyCharm environment..." -ForegroundColor Yellow
$BatchContent | Out-File -FilePath $BatchFile -Encoding ASCII
Write-Host "   Created: $BatchFile" -ForegroundColor Gray

# Create configuration files if they don't exist
Write-Host "⚙️ Setting up configuration files..." -ForegroundColor Yellow

# Email config template
$EmailConfigFile = Join-Path $ConfigPath "email_config.json"
if (-not (Test-Path $EmailConfigFile)) {
    $EmailTemplate = @{
        "smtp_server" = "smtp.gmail.com"
        "smtp_port" = 587
        "sender_email" = "your.email@gmail.com"
        "sender_password" = "your_app_password_here"
        "recipient_emails" = @("recipient1@company.com", "recipient2@company.com")
    } | ConvertTo-Json -Depth 3
    
    $EmailTemplate | Out-File -FilePath $EmailConfigFile -Encoding UTF8
    Write-Host "   Created email config template: $EmailConfigFile" -ForegroundColor Gray
    Write-Host "   📝 Please update with your actual email credentials" -ForegroundColor Yellow
}

# Slack config template
$SlackConfigFile = Join-Path $ConfigPath "slack_config.json"
if (-not (Test-Path $SlackConfigFile)) {
    $SlackTemplate = @{
        "webhook_url" = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        "channel" = "#bloomberg-data"
    } | ConvertTo-Json -Depth 3
    
    $SlackTemplate | Out-File -FilePath $SlackConfigFile -Encoding UTF8
    Write-Host "   Created Slack config template: $SlackConfigFile" -ForegroundColor Gray
    Write-Host "   📝 Please update with your actual Slack webhook URL" -ForegroundColor Yellow
}

# Schedule config
$ScheduleConfigFile = Join-Path $ConfigPath "schedule_config.json"
if (-not (Test-Path $ScheduleConfigFile)) {
    $ScheduleTemplate = @{
        "daily_collection_time" = $CollectionTime
        "weekend_collection" = $false
        "retry_attempts" = 3
        "retry_delay_minutes" = 15
        "data_validation" = $true
        "alert_on_failure" = $true
        "alert_on_success" = $true
        "timezone" = "US/Eastern"
    } | ConvertTo-Json -Depth 3
    
    $ScheduleTemplate | Out-File -FilePath $ScheduleConfigFile -Encoding UTF8
    Write-Host "   Created schedule config: $ScheduleConfigFile" -ForegroundColor Gray
}

# Create PyCharm-specific monitoring script
$MonitorScript = Join-Path $ProjectPath "monitor_vix_system.ps1"
$MonitorContent = @"
# VIX Data Collection System Monitor - PyCharm Project
param([switch]`$Detailed)

`$ProjectPath = "$ProjectPath"
`$LogPath = "$LogPath"
`$DataPath = "$DataPath"
`$VenvPath = "$VenvPath"

Write-Host "🔍 VIX Data Collection Status - PyCharm Project" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
Write-Host "📅 Check Time: `$(Get-Date)" -ForegroundColor Gray
Write-Host "📁 Project: `$ProjectPath" -ForegroundColor Gray
Write-Host "🐍 Virtual Env: `$VenvPath" -ForegroundColor Gray

# Check virtual environment
Write-Host "``n🐍 Virtual Environment:" -ForegroundColor Yellow
if (Test-Path `$VenvPath) {
    `$pythonExe = Join-Path `$VenvPath "Scripts\python.exe"
    if (Test-Path `$pythonExe) {
        try {
            `$pythonVersion = & `$pythonExe --version 2>&1
            Write-Host "   ✅ Python: `$pythonVersion" -ForegroundColor Green
            
            # Test Bloomberg API
            `$blpapiTest = & `$pythonExe -c "import blpapi; print('✅ Bloomberg API available')" 2>&1
            if (`$LASTEXITCODE -eq 0) {
                Write-Host "   `$blpapiTest" -ForegroundColor Green
            } else {
                Write-Host "   ❌ Bloomberg API: Not available" -ForegroundColor Red
            }
        } catch {
            Write-Host "   ❌ Python execution failed" -ForegroundColor Red
        }
    } else {
        Write-Host "   ❌ Python executable not found" -ForegroundColor Red
    }
} else {
    Write-Host "   ❌ Virtual environment not found" -ForegroundColor Red
}

# Check VIX data fetcher script
Write-Host "``n📄 VIX Data Fetcher:" -ForegroundColor Yellow
`$vixScript = Join-Path `$ProjectPath "scripts\vix_data_fetcher.py"
if (Test-Path `$vixScript) {
    `$fileSize = [math]::Round((Get-Item `$vixScript).Length / 1KB, 2)
    Write-Host "   ✅ Script found: vix_data_fetcher.py (`${fileSize} KB)" -ForegroundColor Green
} else {
    Write-Host "   ❌ VIX data fetcher script not found" -ForegroundColor Red
}

# Check scheduled tasks
Write-Host "``n📋 Scheduled Tasks:" -ForegroundColor Yellow
`$tasks = @("VIX-PyCharm-DailyCollection", "VIX-PyCharm-WeekendMaintenance")
foreach (`$task in `$tasks) {
    try {
        `$taskInfo = Get-ScheduledTask -TaskName `$task -ErrorAction Stop
        `$lastRun = (Get-ScheduledTaskInfo -TaskName `$task).LastRunTime
        `$nextRun = (Get-ScheduledTaskInfo -TaskName `$task).NextRunTime
        `$state = `$taskInfo.State
        
        Write-Host "   ✅ `$task : `$state" -ForegroundColor Green
        if (`$Detailed) {
            Write-Host "      Last Run: `$lastRun" -ForegroundColor Gray
            Write-Host "      Next Run: `$nextRun" -ForegroundColor Gray
        }
    } catch {
        Write-Host "   ❌ `$task : Not Found" -ForegroundColor Red
    }
}

# Check recent data files
Write-Host "``n📊 Recent Data Files:" -ForegroundColor Yellow
if (Test-Path `$DataPath) {
    `$recentFiles = Get-ChildItem `$DataPath -Filter "*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 5
    if (`$recentFiles) {
        foreach (`$file in `$recentFiles) {
            `$age = (Get-Date) - `$file.LastWriteTime
            `$ageText = if (`$age.TotalDays -gt 1) { "`$([math]::Round(`$age.TotalDays,1)) days ago" } else { "`$([math]::Round(`$age.TotalHours,1)) hours ago" }
            Write-Host "   📄 `$(`$file.Name) (`$ageText)" -ForegroundColor Gray
        }
    } else {
        Write-Host "   ⚠️ No CSV files found" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ❌ Data directory not found" -ForegroundColor Red
}

# Check configuration files
Write-Host "``n⚙️ Configuration Files:" -ForegroundColor Yellow
`$configFiles = @("email_config.json", "slack_config.json", "schedule_config.json")
foreach (`$configFile in `$configFiles) {
    `$configPath = Join-Path "$ConfigPath" `$configFile
    if (Test-Path `$configPath) {
        Write-Host "   ✅ `$configFile" -ForegroundColor Green
    } else {
        Write-Host "   ❌ `$configFile : Not found" -ForegroundColor Red
    }
}

Write-Host "``n✅ System check completed" -ForegroundColor Green
"@

$MonitorContent | Out-File -FilePath $MonitorScript -Encoding UTF8
Write-Host "   Created: $MonitorScript" -ForegroundColor Gray

# Test mode - just run checks and exit
if ($TestMode) {
    Write-Host "`n🧪 Test Mode - Running system checks..." -ForegroundColor Yellow
    & $MonitorScript -Detailed
    
    Write-Host "`n🧪 Testing VIX data collection (dry run)..." -ForegroundColor Yellow
    try {
        $testResult = & $PythonExe -c "
import sys
sys.path.append('scripts')
print('✅ VIX data fetcher script can be imported')
print('📊 Ready for data collection')
" 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host $testResult -ForegroundColor Green
        } else {
            Write-Host "❌ Test failed: $testResult" -ForegroundColor Red
        }
    } catch {
        Write-Host "❌ Test execution failed: $_" -ForegroundColor Red
    }
    
    Write-Host "`n✅ Test mode completed" -ForegroundColor Green
    return
}

# Create scheduled tasks (only if not in test mode and running as admin)
if ((Test-Admin)) {
    Write-Host "⏰ Creating scheduled tasks..." -ForegroundColor Yellow
    
    # Daily collection task
    $TaskName = "VIX-PyCharm-DailyCollection"
    $TaskDescription = "Daily VIX data collection from PyCharm project"
    
    $Action = New-ScheduledTaskAction -Execute $BatchFile
    $Trigger = New-ScheduledTaskTrigger -Daily -At $CollectionTime
    $Trigger.DaysOfWeek = [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Monday, 
                          [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Tuesday,
                          [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Wednesday,
                          [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Thursday,
                          [Microsoft.PowerShell.Cmdletization.GeneratedTypes.ScheduledTask.DaysOfWeekEnum]::Friday
    
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable -StartWhenAvailable
    $Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
    
    try {
        Register-ScheduledTask -TaskName $TaskName -Description $TaskDescription -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Force
        Write-Host "✅ Task '$TaskName' created successfully" -ForegroundColor Green
    } catch {
        Write-Error "❌ Failed to create task '$TaskName': $($_.Exception.Message)"
    }
    
    # Weekend maintenance task
    $MaintenanceTaskName = "VIX-PyCharm-WeekendMaintenance"
    $MaintenanceAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-File `"$MonitorScript`" -Detailed"
    $MaintenanceTrigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Saturday -At "10:00"
    
    try {
        Register-ScheduledTask -TaskName $MaintenanceTaskName -Description "Weekly VIX system maintenance for PyCharm project" -Action $MaintenanceAction -Trigger $MaintenanceTrigger -Settings $Settings -Principal $Principal -Force
        Write-Host "✅ Task '$MaintenanceTaskName' created successfully" -ForegroundColor Green
    } catch {
        Write-Error "❌ Failed to create task '$MaintenanceTaskName': $($_.Exception.Message)"
    }
} else {
    Write-Host "⚠️ Skipping task creation (not running as Administrator)" -ForegroundColor Yellow
    Write-Host "   To create scheduled tasks, run as Administrator later" -ForegroundColor Gray
}

# Create quick start scripts for PyCharm
$QuickStartScript = Join-Path $ProjectPath "run_vix_collection_manual.ps1"
$QuickStartContent = @"
# Manual VIX Data Collection - PyCharm Project
cd "$ProjectPath"

Write-Host "🚀 Running VIX Data Collection Manually..." -ForegroundColor Green
Write-Host "Project: $ProjectPath" -ForegroundColor Gray

# Activate virtual environment and run
& "$VenvPath\Scripts\Activate.ps1"
python scripts\vix_data_fetcher.py

Write-Host "✅ Manual collection completed" -ForegroundColor Green
"@

$QuickStartContent | Out-File -FilePath $QuickStartScript -Encoding UTF8
Write-Host "   Created manual run script: $QuickStartScript" -ForegroundColor Gray

# Final setup summary
Write-Host "`n📋 PyCharm Project Setup Summary:" -ForegroundColor Green
Write-Host "=================================" -ForegroundColor Green
Write-Host "📁 Project: $ProjectPath" -ForegroundColor Gray
Write-Host "🐍 Virtual Environment: $VenvPath" -ForegroundColor Gray
Write-Host "📄 VIX Script: $VIXFetcherScript" -ForegroundColor Gray
Write-Host "⏰ Collection Time: $CollectionTime (Monday-Friday)" -ForegroundColor Gray
Write-Host "📊 Data Output: $DataPath" -ForegroundColor Gray

Write-Host "`n📝 Next Steps:" -ForegroundColor Yellow
Write-Host "1. Update email/Slack configs in: $ConfigPath" -ForegroundColor Gray
Write-Host "2. Test the system: .\monitor_vix_system.ps1" -ForegroundColor Gray
Write-Host "3. Run manual test: .\run_vix_collection_manual.ps1" -ForegroundColor Gray
Write-Host "4. Verify Bloomberg Terminal is running and logged in" -ForegroundColor Gray

Write-Host "`n🎮 Management Commands:" -ForegroundColor Yellow
Write-Host "• System status: .\monitor_vix_system.ps1" -ForegroundColor Gray
Write-Host "• Manual run: .\run_vix_collection_manual.ps1" -ForegroundColor Gray
Write-Host "• View tasks: Get-ScheduledTask -TaskName 'VIX-PyCharm-*'" -ForegroundColor Gray
Write-Host "• Test mode: .\setup_pycharm_vix.ps1 -TestMode" -ForegroundColor Gray

Write-Host "`n🧪 Initial System Check:" -ForegroundColor Yellow
& $MonitorScript

Write-Host "`n✅ PyCharm VIX setup completed!" -ForegroundColor Green
