Import-Module 'C:\Program Files (x86)\Microsoft SDKs\Windows Azure\PowerShell\ServiceManagement\Azure\Azure.psd1'

Add-Type @'
public class NaiadAccounts
{
    public System.Management.Automation.PSCredential credential;
    public System.String storageAccountName;
    public System.String cloudServiceName;
}
'@





function Naiad-ConfigureAccounts
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$adminName,
        [parameter(Mandatory=$true)]
        [SecureString]$secPassword,
        [parameter(Mandatory=$true)]
        [string]$storageAccountName,
        [parameter(Mandatory=$true)]
        [string]$cloudServiceName
    )
    PROCESS {
        
        $result = New-Object NaiadAccounts;

        $result.credential = New-Object System.Management.Automation.PSCredential($adminName, $secPassword)
        $result.storageAccountName = $storageAccountName
        $result.cloudServiceName = $cloudServiceName

        $result
    }
}

# wraps password into credential
$adminName = "<put the admin username here>"
$adminPass = "<put the admin password here>"
$secPassword = ConvertTo-SecureString $adminPass -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential($adminName, $secPassword)

$accountInformation = Naiad-ConfigureAccounts $adminName $secPassword "<put the storage account name here>" "<put the cloud service name here>"


function Naiad-PrepareJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [string]$pathToExecutable,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
    )
    PROCESS {

        $guid = [guid]::NewGuid()

        $package = "C:\temp\" + $guid + ".zip"

        $loadresult = [Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem")
        $compressResult = [System.IO.Compression.ZipFile]::CreateFromDirectory($pathToExecutable, $package)

        # push the zipped file to Azure blob store.
        $storageKey = Get-AzureStorageKey -StorageAccountName $accountInformation.storageAccountName
        $storageContext = New-AzureStorageContext -StorageAccountName $accountInformation.storageAccountName -StorageAccountKey $storageKey.Primary
        $container = Get-AzureStorageContainer -Context $storageContext -Name "executables"
        $blob = Set-AzureStorageBlobContent -Context $storageContext -Container $container.Name -Blob $guid -File $package

        $guid
    }
}

# e.g. to prepare a job
$guid = Naiad-PrepareJob "C:\users\MyName\Source\Repos\MySolution\bin\Release" $accountInformation

#deploys a Naiad job from a guid and account information
function Naiad-DeployJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [Guid]$guid,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
    )
    PROCESS {

        # get the URIs to talk to all the VMs in the cloud service
        $uris = Get-AzureWinRMUri -ServiceName $accountInformation.cloudServiceName 

        # find the zipped file in Azure blob store.
        $storageKey = Get-AzureStorageKey -StorageAccountName $accountInformation.storageAccountName
        $storageContext = New-AzureStorageContext -StorageAccountName $accountInformation.storageAccountName -StorageAccountKey $storageKey.Primary
        $container = Get-AzureStorageContainer -Context $storageContext -Name "executables"
        $blob = Get-AzureStorageBlob -Context $storageContext -Container $container.Name -Blob $guid
        $bloburi = $blob.ICloudBlob.Uri

        # for each VM, if the zip file isn't already there, download it
        $deployJobs = @()
        $uris | ForEach-Object { Invoke-Command -ScriptBlock { 
                $bloburi = $args[0]
                $guid = $args[1]

                if (Test-Path "C:\temp\$guid")
                {
                    # already uploaded, 
                }
                else
                {
                    wget $args[0] -UseBasicParsing -OutFile "C:\temp\$guid.zip"
                    [Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem")
                    [System.IO.Compression.ZipFile]::ExtractToDirectory("C:\temp\$guid.zip", "C:\temp\$guid\")
                }
    
            } -ConnectionUri $_ -AsJob -Credential $accountInformation.credential -SessionOption (New-PSSessionOption -SkipCACheck) -ArgumentList $bloburi, $guid
        }
    }
}

# copy the job zip to all the VMs
$deployJobs = Naiad-DeployJob $guid $accountInformation
# and wait for it to complete
Wait-Job $deployJobs -Timeout 10
# and get the results
$deployOutput = $deployJobs | ForEach-Object { Receive-Job -Job $_ } 


# executes a Naiad job from a guid, account information, and executable string
function Naiad-ExecuteJob
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [Guid]$guid,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation,
        [parameter(Mandatory=$true)]
        [string]$executableName,
        [parameter(Mandatory=$true)]
        [string]$additionalArguments
    )
    PROCESS {

        # get the URIs to talk to all the VMs in the cloud service
        $uris = Get-AzureWinRMUri -ServiceName $accountInformation.cloudServiceName 

        # make a mapping that assigns each VM name an integer
        $i = 0
        $mapping = @{}
        $result = Get-AzureVM -ServiceName $accountInformation.cloudServiceName | ForEach-Object { $mapping[$_.InstanceName.ToLower()] = $i++ }

        # make the reverse mapping from integers to VM names
        $revMapping = @{}
        $mapping.GetEnumerator() | ForEach-Object { $revMapping[$_.Value] = $_.Key }

        # make the connection string that the executables will need to talk to blob storage
        $storageKey = Get-AzureStorageKey -StorageAccountName $accountInformation.storageAccountName
        $connectionString = "DefaultEndpointsProtocol=http;AccountName=" + $accountInformation.storageAccountName + ";AccountKey=" + $storageKey.Primary + ";"

        $uris | ForEach-Object {
            Invoke-Command -ScriptBlock { 
        
                $guid = $args[1]
                $mapping = $args[2]
                $revMapping = $args[3]
                $connectionString = $args[4]
                $executableName = $args[5]
                $additionalArguments = $args[6]

                $argList = $additionalArguments.Split()

                $procID = $mapping[$env:COMPUTERNAME.ToLower()]
    
                # augment the arguments with default Naiad args: you may want to edit this. It assumes 7 threads per process
                $argList += @("-n", $mapping.Count, "-t", "7", "--addsetting",  "Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString", $connectionString, "--inlineserializer", "-p", $procID, "-h")
                # supply the listener ports for all the processes in the job
                (0..($mapping.Count-1)) | ForEach-Object { $argList += $revMapping[$_] + ":2101 " }


                cd c:\temp\$guid\

                & "C:\temp\$guid\$executableName" $argList 2>&1 | %{ "$_".TrimEnd() }
                #& "C:\temp\$guid\$executableName" $argList 2>&1 | %{ "$_".TrimEnd() } > C:\temp\$guid\output.txt
            } -AsJob -ConnectionUri $_ -Credential $accountInformation.credential -SessionOption (New-PSSessionOption -SkipCACheck) -ArgumentList $bloburi,$guid,$mapping,$revMapping,$connectionString,$executableName,$additionalArguments
        }
    }
}

# e.g. to run a particular job
$executeJobs = Naiad-ExecuteJob $guid $accountInformation "MyProgram.exe" "MyFirstArge MySecondArg"

$outputMap = @{}

# get all the outputs, and show the output of the first process
(0..($executeJobs.Length-1)) | ForEach-Object { $outputMap[$_] += (Receive-Job -Job $executeJobs[$_] 2>&1 | %{ "$_".TrimEnd() }) }
$outputMap[0]

function Naiad-ConnectRemoteDesktop 
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation,
        [parameter(Mandatory=$true)]
        [string]$hostname
    )
    PROCESS {
        Get-AzureRemoteDesktopFile -ServiceName $accountInformation.cloudServiceName -Name $hostname -Launch
    }
}




# e.g. to see the order that process 25 in a job received shutdown messages from its peers
$outputMap[25] | Select-String -AllMatches -Pattern "Received shutdown message from [0-9]+" | Select-Object -ExpandProperty Matches | Select-Object -ExpandProperty Value | sort 

Naiad-StopAzureProcess Twiral $accountInformation

# stop all the processes with a given name on the VMs
function Naiad-StopAzureProcess
{
    [CmdletBinding()]
    param (

        [parameter(Mandatory=$true)]
        [string]$processname,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
        
    )
    PROCESS {

        $uris = Get-AzureWinRMUri -ServiceName $accountInformation.cloudServiceName 
        $jobs = $uris | ForEach-Object { Invoke-Command -AsJob -ConnectionUri $_ -ScriptBlock { Stop-Process -Name $args[0] } -Credential $accountInformation.credential -SessionOption (New-PSSessionOption -SkipCACheck) -ArgumentList $processname }

        Wait-Job $jobs
    }
}

# get the memory being used by each process
function Naiad-GetAzureProcessMemory
{
    [CmdletBinding()]
    param (

        [parameter(Mandatory=$true)]
        [string]$processname,
        [parameter(Mandatory=$true)]
        [NaiadAccounts]$accountInformation
        
    )
    PROCESS {

        $uris = Get-AzureWinRMUri -ServiceName $accountInformation.cloudServiceName 
        $jobs = $uris | ForEach-Object { Invoke-Command -AsJob -ConnectionUri $_ -ScriptBlock { (Get-Process -Name $args[0]).PagedMemorySize64 } -Credential $accountInformation.credential -SessionOption (New-PSSessionOption -SkipCACheck) -ArgumentList $processname }

        Wait-Job $jobs
    }
}


# identify the blob uri, and the uris which will want it
$cloudService = "<put cloud service name here>"
$uris = Get-AzureWinRMUri -ServiceName $cloudService 

# update the powershell quotas on each VM to be allowed to use more than 1GB: otherwise powershell will silently kill any process that
# exceeds this amount of memory
$configureJobs = @()
$uris | ForEach-Object {
    $configureJobs += Invoke-Command -ScriptBlock {

        # netsh firewall add portopening TCP 2101 "Naiad"

        Set-Item WSMan:\localhost\Shell\MaxMemoryPerShellMB 16000 -Force
        Set-Item WSMan:\localhost\Plugin\microsoft.powershell\Quotas\MaxMemoryPerShellMB 16000
    
        Restart-Service winrm
    
    } -AsJob -ConnectionUri $_ -Credential $credential -SessionOption (New-PSSessionOption -SkipCACheck)
}

Wait-Job $configureJobs
Receive-Job -Job $configureJobs
