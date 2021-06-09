# Backblaze Flysystem Driver

[![Author](http://img.shields.io/badge/author-@marc_andre-blue.svg?style=flat-square)](https://twitter.com/marc_andre)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/marcandreappel/flysystem-backblaze.svg?style=flat-square)](https://packagist.org/packages/marcandreappel/flysystem-backblaze)
[![Software License][ico-license]](LICENSE.md)
[![Total Downloads](https://img.shields.io/packagist/dt/marcandreappel/flysystem-backblaze.svg?style=flat-square)](https://packagist.org/packages/marcandreappel/flysystem-backblaze)

Visit (https://secure.backblaze.com/b2_buckets.htm) and get your account id and application key.

The Backblaze adapter makes it possible to use the Flysystem filesystem abstraction library with Backblaze. 
It uses the [Backblaze B2 SDK](https://github.com/cwhite92/b2-sdk-php) to communicate with the API.

## Install

Via Composer

```bash
composer require marcandreappel/flysystem-backblaze
```

## Usage

```php
use MarcAndreAppel\FlysystemBackblaze\BackblazeAdapter;
use League\Flysystem\Filesystem;
use BackblazeB2\Client;

$client  = new Client($accountId, $applicationKey);
$adapter = new BackblazeAdapter($client, $bucketName);

$filesystem = new Filesystem($adapter);
```

## Using ApplicationKey instead of MasterKey

If you specify only the `$bucketName` when creating the BackblazeAdapter, your `$applicationKey` must be the **master key**.
However, if you specify both bucket name and bucket id, you can use an application key.
Fetch your `$bucketId` using the [b2 command line tool](https://www.backblaze.com/b2/docs/quick_command_line.html) `b2 get-bucket <bucketName>` 

```php
$client  = new Client($accountId, $applicationKey);
$adapter = new BackblazeAdapter($client, $bucketName, $bucketId);
```

## Documentation
Here is the [complete guide](https://flysystem.thephpleague.com/docs/usage/filesystem-api/) of all available options.

## Security

If you discover any security related issues, please open a ticket on the issue tracker.

## Credits

- [Ramesh Mhetre][link-author]
- [All Contributors][link-contributors]

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.

[ico-license]: https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square
[link-contributors]: ../../contributors
