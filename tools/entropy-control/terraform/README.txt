Install Terraform and AWS cli tool.

Set up AWS confidence with `aws configure`.

Put `ephemeral.pem` in `~/.ssh`. Add the following section to `~/.ssh/config`
```
Host *.compute.amazonaws.com
    User ubuntu
    IdentityFile ~/.ssh/ephemeral.pem
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
    LogLevel Quiet
```

Run commands in this directory, or add `-chdir=/path/to/this/dir` argument.

Initialize Terraform module by run this for once
$ terraform init

Create evaluation infrastracture in 5 regions, each region has 10 instances
$ terraform apply -var mode=five -var instance-count=10

(Type "yes" when prompt.)

Notice: always repeat `terraform apply ...` commands until it says "No changes." and not prompting.

The infrastracture set up a subnet in each region, and spawn all instances in that subnet. All ports are open to the Internet. Public IP addresses are allocated to instances.

Get the list of public DNS names of instances
$ terraform output -json | jq -r '.instances.value[].public_dns'

SSH to any of the instances with its DNS name.

Shutdown instances
$ terraform apply -var mode=five -var instance-count=10 -var state=stopped

Repeat the original `terraform apply ...` to restart instances.

To modify infrastracture specification: repeat the original `terraform apply ...` with different values. For example,  create evaluation infrastracture in 1 region with 10 instances
$ terraform apply -var mode=one -var instance-count=10

Destroy instances
$ terraform destroy -var mode=five -var instance-count=10

The mode and instance count must be identical to the last `terraform apply ...` command.

Additional notes on IPFS. By default, network interfaces on EC2 instances does not bind its public IP. In order for IPFS node to establish bidirectional connections, you have to manually do the binding
$ sudo ip addr add <PUBLIC IP> dev ens5

Notice that instance public IPs change every time `terraform apply ...` is executed (which is possibly reason why it is not bound to network interfaces by default). So the procedure above has to be repeated per applying.

IPFS node must listen to *both* public and private IPs in order to connect to each other under any condition. To set up a node that listen on the default port 4000
$ ipfs init --profile server
$ ipfs config Addresses.Swarm --json '["/ip4/<PUBLIC IP>/tcp/4000", "/ip4/<PRIVATE IP>/tcp/4000]'
$ ipfs daemon
